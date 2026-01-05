"""
Analyze HN-only event detection
Focus on what actually works
"""
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from scipy.stats import ttest_ind
import numpy as np

print("="*70)
print("HN EVENT ANALYSIS")
print("="*70)

events = [
    {'name': 'Reddit API Blackout', 'date': '2024-06-12'},
    {'name': 'Trump Assassination', 'date': '2024-07-13'},
]

dimensions = [
    'emotional_valence_shift', 'temporal_bleed', 'certainty_collapse',
    'time_compression', 'agency_reversal', 'metaphor_cluster_density',
    'novel_meme_explosion', 'sacred_profane_ratio', 'pronoun_flip'
]

pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)

# Calculate 2024 baselines
print("\nCalculating 2024 baselines...")
baselines = {}
for dim in dimensions:
    query = f"""
        SELECT AVG(b.score) as baseline, STDDEV(b.score) as std
        FROM bert_{dim} b
        JOIN stories s ON b.story_id = s.id
        WHERE DATE(s.created_at) BETWEEN '2024-01-01' AND '2024-11-30'
    """
    df = pd.read_sql(query, pg_conn)
    baselines[dim] = {
        'mean': df['baseline'].values[0],
        'std': df['std'].values[0]
    }

# Analyze each event
for event in events:
    event_date = datetime.strptime(event['date'], '%Y-%m-%d')
    
    # 7-day window before event
    predict_start = event_date - timedelta(days=7)
    predict_end = event_date - timedelta(days=1)
    
    print(f"\n{'='*70}")
    print(f"{event['name']} - {event_date.date()}")
    print(f"Predictive window: {predict_start.date()} to {predict_end.date()}")
    print(f"{'='*70}\n")
    
    for dim in dimensions:
        # Get scores in predictive window
        query = f"""
            SELECT b.score
            FROM bert_{dim} b
            JOIN stories s ON b.story_id = s.id
            WHERE DATE(s.created_at) BETWEEN %s AND %s
        """
        window_df = pd.read_sql(query, pg_conn, params=(predict_start, predict_end))
        
        if len(window_df) == 0:
            continue
            
        window_mean = window_df['score'].mean()
        baseline_mean = baselines[dim]['mean']
        baseline_std = baselines[dim]['std']
        
        # Calculate change
        change_pct = ((window_mean - baseline_mean) / baseline_mean * 100)
        
        # Z-score (how many standard deviations from baseline)
        z_score = (window_mean - baseline_mean) / baseline_std
        
        # Percentile
        percentile = (1 + z_score / np.sqrt(1 + z_score**2)) * 50
        
        # Get all 2024 scores for t-test
        baseline_query = f"""
            SELECT b.score
            FROM bert_{dim} b
            JOIN stories s ON b.story_id = s.id
            WHERE DATE(s.created_at) BETWEEN '2024-01-01' AND '2024-11-30'
        """
        baseline_df = pd.read_sql(baseline_query, pg_conn)
        
        # T-test
        t_stat, p_value = ttest_ind(window_df['score'], baseline_df['score'])
        
        # Flag if significant
        significant = "🔴" if abs(change_pct) > 10 and p_value < 0.05 else "  "
        
        print(f"{dim:30s} {significant} | "
              f"Window: {window_mean:.4f} | "
              f"Baseline: {baseline_mean:.4f} | "
              f"Change: {change_pct:+6.1f}% | "
              f"Percentile: {percentile:.1f} | "
              f"p={p_value:.4f}")

pg_conn.close()

print("\n" + "="*70)
print("ANALYSIS COMPLETE")
print("="*70)