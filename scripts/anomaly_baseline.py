"""
Establish baseline distributions for each dimension
Determine what constitutes an 'anomaly' vs normal variation
"""
import pandas as pd
import psycopg2
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

print("="*70)
print("BASELINE ANOMALY DETECTION")
print("="*70)

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='linguistic_predictor_v2',
    user='analyzer',
    password='dev_password_change_in_prod'
)

# Get full year of daily scores
dimensions = [
    'emotional_valence_shift',
    'temporal_bleed',
    'certainty_collapse',
    'time_compression',
    'agency_reversal'
]

start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 5)

all_daily_data = []

for dimension in dimensions:
    print(f"Loading {dimension}...")
    query = f"""
        SELECT 
            DATE(s.created_at) as date,
            AVG(b.score) as avg_score,
            COUNT(*) as count
        FROM bert_{dimension} b
        JOIN stories s ON b.story_id = s.id
        WHERE s.created_at BETWEEN %s AND %s
        GROUP BY DATE(s.created_at)
        ORDER BY date
    """
    
    df = pd.read_sql(query, conn, params=(start_date, end_date))
    df['dimension'] = dimension
    all_daily_data.append(df)

conn.close()

daily_df = pd.concat(all_daily_data, ignore_index=True)
daily_df['date'] = pd.to_datetime(daily_df['date'])

print(f"\nLoaded {len(daily_df)} daily observations")

# Calculate baseline statistics for each dimension
baseline_stats = []

for dimension in dimensions:
    dim_data = daily_df[daily_df['dimension'] == dimension]
    
    scores = dim_data['avg_score'].values
    
    stats_dict = {
        'dimension': dimension,
        'mean': np.mean(scores),
        'std': np.std(scores),
        'median': np.median(scores),
        'p50': np.percentile(scores, 50),
        'p75': np.percentile(scores, 75),
        'p90': np.percentile(scores, 90),
        'p95': np.percentile(scores, 95),
        'p99': np.percentile(scores, 99),
        'max': np.max(scores),
        'n_days': len(scores)
    }
    
    baseline_stats.append(stats_dict)

baseline_df = pd.DataFrame(baseline_stats)

print("\n" + "="*70)
print("BASELINE DISTRIBUTIONS (Full Year 2024)")
print("="*70)

for _, row in baseline_df.iterrows():
    print(f"\n{row['dimension']}:")
    print(f"  Mean: {row['mean']:.4f}")
    print(f"  Std:  {row['std']:.4f}")
    print(f"  75th percentile: {row['p75']:.4f}")
    print(f"  90th percentile: {row['p90']:.4f}")
    print(f"  95th percentile: {row['p95']:.4f} ← Anomaly threshold")
    print(f"  99th percentile: {row['p99']:.4f}")
    print(f"  Max: {row['max']:.4f}")

# Now analyze Reddit event against baseline
event_date = datetime(2024, 6, 12)

print("\n" + "="*70)
print("REDDIT EVENT ANALYSIS vs BASELINE")
print("="*70)

event_analysis = []

for dimension in dimensions:
    baseline_row = baseline_df[baseline_df['dimension'] == dimension].iloc[0]
    
    # Get scores around event
    dim_daily = daily_df[daily_df['dimension'] == dimension].copy()
    
    # 7 days before event
    warning_period = dim_daily[
        (dim_daily['date'] >= event_date - timedelta(days=7)) &
        (dim_daily['date'] < event_date)
    ]
    
    # Event day
    event_day = dim_daily[dim_daily['date'] == event_date]
    
    if len(warning_period) > 0 and len(event_day) > 0:
        warning_max = warning_period['avg_score'].max()
        warning_max_date = warning_period[warning_period['avg_score'] == warning_max]['date'].values[0]
        event_score = event_day['avg_score'].values[0]
        
        # Calculate percentile rank of warning peak
        all_scores = dim_daily['avg_score'].values
        warning_percentile = (all_scores < warning_max).sum() / len(all_scores) * 100
        event_percentile = (all_scores < event_score).sum() / len(all_scores) * 100
        
        # Days before event
        days_before = (event_date - pd.to_datetime(warning_max_date)).days
        
        event_analysis.append({
            'dimension': dimension,
            'warning_peak_score': warning_max,
            'warning_peak_date': str(warning_max_date)[:10],
            'days_before_event': days_before,
            'warning_percentile': warning_percentile,
            'event_score': event_score,
            'event_percentile': event_percentile,
            'p95_threshold': baseline_row['p95'],
            'warning_above_p95': warning_max > baseline_row['p95'],
            'event_above_p95': event_score > baseline_row['p95'],
            'warning_above_p90': warning_max > baseline_row['p90'],
            'event_above_p90': event_score > baseline_row['p90']
        })

event_df = pd.DataFrame(event_analysis)

print("\nWARNING PERIOD PEAKS (7 days before event):")
for _, row in event_df.iterrows():
    print(f"\n{row['dimension']}:")
    print(f"  Peak: {row['warning_peak_score']:.4f} on {row['warning_peak_date']} ({row['days_before_event']} days before)")
    print(f"  Percentile rank: {row['warning_percentile']:.1f}%")
    
    if row['warning_above_p95']:
        print(f"  ⚠️  ANOMALY: Above 95th percentile ({row['p95_threshold']:.4f})")
    elif row['warning_above_p90']:
        print(f"  ⚡ HIGH: Above 90th percentile")
    else:
        print(f"  ✓ Normal variation")

print("\nEVENT DAY:")
for _, row in event_df.iterrows():
    print(f"\n{row['dimension']}:")
    print(f"  Score: {row['event_score']:.4f}")
    print(f"  Percentile rank: {row['event_percentile']:.1f}%")
    
    if row['event_above_p95']:
        print(f"  ⚠️  ANOMALY: Above 95th percentile")
    elif row['event_above_p90']:
        print(f"  ⚡ HIGH: Above 90th percentile")

# Save results
baseline_df.to_csv('data/baseline_distributions.csv', index=False)
event_df.to_csv('data/reddit_event_vs_baseline.csv', index=False)

print("\n" + "="*70)
print("PREDICTIVE CAPABILITY ASSESSMENT")
print("="*70)

predictive_signals = event_df[event_df['warning_above_p95'] == True]

print(f"\nDimensions with P95 anomalies BEFORE event: {len(predictive_signals)}/{len(event_df)}")

if len(predictive_signals) > 0:
    print("\n✓ PREDICTIVE SIGNALS DETECTED:")
    for _, row in predictive_signals.iterrows():
        print(f"  - {row['dimension']}: {row['days_before_event']} days advance warning")
    
    print("\n📊 This system CAN provide advance warning of events")
else:
    print("\n✗ NO PREDICTIVE SIGNALS")
    print("  Warning period peaks were within normal variation")
    print("  System detected event retrospectively only")

print("\n✓ Saved:")
print("  - data/baseline_distributions.csv")
print("  - data/reddit_event_vs_baseline.csv")