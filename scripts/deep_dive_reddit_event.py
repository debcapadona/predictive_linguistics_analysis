"""
Deep dive analysis of Reddit API Blackout event
Statistical testing, control periods, temporal analysis
"""
import pandas as pd
import psycopg2
import numpy as np
from datetime import datetime, timedelta
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns

print("="*70)
print("DEEP DIVE: Reddit API Blackout (June 12, 2024)")
print("="*70)

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='linguistic_predictor_v2',
    user='analyzer',
    password='dev_password_change_in_prod'
)

event_date = datetime(2024, 6, 12)
event_window = 3  # days

# Define analysis periods
periods = {
    'event': {
        'start': event_date - timedelta(days=event_window),
        'end': event_date + timedelta(days=event_window),
        'label': 'Event Window'
    },
    'baseline': {
        'start': event_date - timedelta(days=33),
        'end': event_date - timedelta(days=4),
        'label': 'Baseline (30 days prior)'
    }
}

# Add 5 random control periods (same length as event window)
np.random.seed(42)
for i in range(5):
    # Pick random date at least 45 days away from event
    days_offset = np.random.randint(45, 150)
    control_start = event_date - timedelta(days=days_offset)
    periods[f'control_{i+1}'] = {
        'start': control_start - timedelta(days=event_window),
        'end': control_start + timedelta(days=event_window),
        'label': f'Control Period {i+1}'
    }

dimensions = [
    'emotional_valence_shift',
    'temporal_bleed',
    'certainty_collapse',
    'time_compression',
    'agency_reversal'
]

print("\n1. COLLECTING DATA FOR ALL PERIODS...")

results = []

for period_name, period_info in periods.items():
    for dimension in dimensions:
        query = f"""
            SELECT b.score
            FROM bert_{dimension} b
            JOIN stories s ON b.story_id = s.id
            WHERE s.created_at BETWEEN %s AND %s
        """
        
        df = pd.read_sql(query, conn, 
                        params=(period_info['start'], period_info['end']))
        
        if len(df) > 0:
            results.append({
                'period': period_name,
                'period_label': period_info['label'],
                'dimension': dimension,
                'mean': df['score'].mean(),
                'std': df['score'].std(),
                'median': df['score'].median(),
                'count': len(df),
                'scores': df['score'].values
            })

results_df = pd.DataFrame(results)

print("\n2. STATISTICAL TESTING: Event vs Baseline vs Controls")
print("="*70)

stat_tests = []

for dimension in dimensions:
    event_scores = results_df[
        (results_df['period'] == 'event') & 
        (results_df['dimension'] == dimension)
    ]['scores'].values[0]
    
    baseline_scores = results_df[
        (results_df['period'] == 'baseline') & 
        (results_df['dimension'] == dimension)
    ]['scores'].values[0]
    
    # T-test: Event vs Baseline
    t_stat, p_value = stats.ttest_ind(event_scores, baseline_scores)
    
    # Effect size (Cohen's d)
    pooled_std = np.sqrt((np.std(event_scores)**2 + np.std(baseline_scores)**2) / 2)
    cohens_d = (np.mean(event_scores) - np.mean(baseline_scores)) / pooled_std
    
    # Compare to control periods
    control_means = []
    for i in range(5):
        control_scores = results_df[
            (results_df['period'] == f'control_{i+1}') & 
            (results_df['dimension'] == dimension)
        ]['scores'].values[0]
        control_means.append(np.mean(control_scores))
    
    event_mean = np.mean(event_scores)
    baseline_mean = np.mean(baseline_scores)
    
    # Is event mean outside range of controls?
    control_min = min(control_means)
    control_max = max(control_means)
    is_anomalous = event_mean > control_max or event_mean < control_min
    
    stat_tests.append({
        'dimension': dimension,
        'event_mean': event_mean,
        'baseline_mean': baseline_mean,
        'pct_change': ((event_mean - baseline_mean) / baseline_mean) * 100,
        't_statistic': t_stat,
        'p_value': p_value,
        'cohens_d': cohens_d,
        'control_min': control_min,
        'control_max': control_max,
        'is_anomalous': is_anomalous,
        'significant': p_value < 0.05
    })
    
    print(f"\n{dimension}:")
    print(f"  Event mean: {event_mean:.4f}")
    print(f"  Baseline mean: {baseline_mean:.4f}")
    print(f"  Change: {((event_mean - baseline_mean) / baseline_mean) * 100:+.1f}%")
    print(f"  T-statistic: {t_stat:.3f}")
    print(f"  P-value: {p_value:.4f} {'***' if p_value < 0.001 else '**' if p_value < 0.01 else '*' if p_value < 0.05 else 'ns'}")
    print(f"  Cohen's d: {cohens_d:.3f} ({'large' if abs(cohens_d) > 0.8 else 'medium' if abs(cohens_d) > 0.5 else 'small'})")
    print(f"  Control range: [{control_min:.4f}, {control_max:.4f}]")
    print(f"  Anomalous: {'YES' if is_anomalous else 'NO'}")

stat_df = pd.DataFrame(stat_tests)
stat_df.to_csv('data/reddit_event_statistical_tests.csv', index=False)

print("\n3. TIME SERIES ANALYSIS")
print("="*70)

# Daily scores 14 days before and after
start = event_date - timedelta(days=14)
end = event_date + timedelta(days=14)

daily_data = []

for dimension in dimensions:
    query = f"""
        SELECT 
            DATE(s.created_at) as date,
            AVG(b.score) as avg_score,
            STDDEV(b.score) as std_score,
            COUNT(*) as count
        FROM bert_{dimension} b
        JOIN stories s ON b.story_id = s.id
        WHERE s.created_at BETWEEN %s AND %s
        GROUP BY DATE(s.created_at)
        ORDER BY date
    """
    
    df = pd.read_sql(query, conn, params=(start, end))
    df['dimension'] = dimension
    daily_data.append(df)

daily_df = pd.concat(daily_data, ignore_index=True)
daily_df.to_csv('data/reddit_event_daily_timeseries.csv', index=False)

print(f"  ✓ Saved daily time series to data/reddit_event_daily_timeseries.csv")

print("\n4. TOPIC ANALYSIS: What topics spiked during event?")
print("="*70)

# Get topics that appeared during event window
topic_query = """
    SELECT 
        t1.topic_name as domain,
        t2.topic_name as category,
        t3.topic_name as topic,
        COUNT(*) as mentions
    FROM comment_labels cl
    JOIN stories s ON cl.comment_id::text = s.id
    JOIN topic_taxonomy t3 ON cl.topic_id = t3.id
    JOIN topic_taxonomy t2 ON t3.parent_id = t2.id
    JOIN topic_taxonomy t1 ON t2.parent_id = t1.id
    WHERE s.created_at BETWEEN %s AND %s
    AND cl.label_type = 'topic'
    AND t3.tier = 3
    GROUP BY t1.topic_name, t2.topic_name, t3.topic_name
    ORDER BY mentions DESC
    LIMIT 20
"""

topic_df = pd.read_sql(topic_query, conn, 
                       params=(periods['event']['start'], 
                              periods['event']['end']))

print("\nTop 20 topics during Reddit API Blackout:")
for idx, row in topic_df.iterrows():
    print(f"  {row['mentions']:4d} - {row['topic'][:60]}")

topic_df.to_csv('data/reddit_event_top_topics.csv', index=False)

conn.close()

print("\n" + "="*70)
print("SUMMARY")
print("="*70)

sig_dims = stat_df[stat_df['significant'] == True]
print(f"\nStatistically significant changes (p < 0.05): {len(sig_dims)}/{len(dimensions)}")

for _, row in sig_dims.iterrows():
    symbol = "↑" if row['pct_change'] > 0 else "↓"
    print(f"  {symbol} {row['dimension']}: {row['pct_change']:+.1f}% (p={row['p_value']:.4f}, d={row['cohens_d']:.2f})")

print("\n✓ Analysis complete! Files saved:")
print("  - data/reddit_event_statistical_tests.csv")
print("  - data/reddit_event_daily_timeseries.csv")
print("  - data/reddit_event_top_topics.csv")