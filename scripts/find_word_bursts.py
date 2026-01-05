"""
Find words that burst during a specific time period
Compares word frequency in window vs baseline
"""
import psycopg2
from datetime import datetime
import pandas as pd

print("="*70)
print("WORD BURST DETECTION")
print("="*70)

# Time window
window_start = '2024-08-01'
window_end = '2024-08-10'

print(f"\nAnalyzing window: {window_start} to {window_end}")

pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)

# Get word frequencies in window
window_query = """
    SELECT 
        wt.word_text,
        COUNT(*) as window_count
    FROM word_tokens wt
    JOIN stories s ON wt.story_id = s.id
    WHERE DATE(s.created_at) BETWEEN %s AND %s
    AND LENGTH(wt.word_text) > 3
    GROUP BY wt.word_text
    HAVING COUNT(*) > 5
"""

window_df = pd.read_sql(window_query, pg_conn, params=(window_start, window_end))
print(f"  ✓ Found {len(window_df)} unique words in window")

# Get baseline frequencies (rest of 2024, excluding window)
baseline_query = """
    SELECT 
        wt.word_text,
        COUNT(*) as baseline_count,
        COUNT(DISTINCT DATE(s.created_at)) as days_present
    FROM word_tokens wt
    JOIN stories s ON wt.story_id = s.id
    WHERE DATE(s.created_at) BETWEEN '2024-01-01' AND '2024-11-30'
    AND DATE(s.created_at) NOT BETWEEN %s AND %s
    AND LENGTH(wt.word_text) > 3
    GROUP BY wt.word_text
"""

baseline_df = pd.read_sql(baseline_query, pg_conn, params=(window_start, window_end))
print(f"  ✓ Found {len(baseline_df)} unique words in baseline")

# Merge and calculate burst score
merged = window_df.merge(baseline_df, on='word_text', how='inner')

# Normalize by number of days
window_days = 10
baseline_days = 334  # ~365 - 31 (approximate)

merged['window_daily_rate'] = merged['window_count'] / window_days
merged['baseline_daily_rate'] = merged['baseline_count'] / baseline_days

# Burst score = (window_rate - baseline_rate) / baseline_rate
merged['burst_score'] = ((merged['window_daily_rate'] - merged['baseline_daily_rate']) 
                        / merged['baseline_daily_rate'])

# Filter for significant bursts
bursts = merged[
    (merged['burst_score'] > 2.0) &  # 200%+ increase
    (merged['window_count'] > 10)     # Minimum volume
].sort_values('burst_score', ascending=False)

print(f"\n{'='*70}")
print(f"TOP BURSTING WORDS (Aug 1-10, 2024)")
print(f"{'='*70}\n")

print(f"{'Word':<20} {'Window':<10} {'Baseline':<10} {'Burst Score':<12}")
print(f"{'-'*20} {'-'*10} {'-'*10} {'-'*12}")

for _, row in bursts.head(50).iterrows():
    print(f"{row['word_text']:<20} {row['window_count']:>9.0f} "
          f"{row['baseline_daily_rate']*window_days:>9.1f} "
          f"{row['burst_score']:>11.1f}x")

# Save results
bursts.to_csv('data/word_bursts_aug1_10.csv', index=False)
print(f"\n✓ Saved: data/word_bursts_aug1_10.csv")

pg_conn.close()