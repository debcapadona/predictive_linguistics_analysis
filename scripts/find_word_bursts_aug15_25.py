"""
Find word bursts/drops during Aug 15-25 (the dip period)
Shows both spikes and drops compared to baseline
"""
import psycopg2
from datetime import datetime
import pandas as pd

print("="*70)
print("WORD BURST/DROP DETECTION - AUG 15-25")
print("="*70)

# Time window (the dip period)
window_start = '2024-08-15'
window_end = '2024-08-25'

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
    HAVING COUNT(*) > 50  -- Only common words
"""

baseline_df = pd.read_sql(baseline_query, pg_conn, params=(window_start, window_end))
print(f"  ✓ Found {len(baseline_df)} unique words in baseline")

# Merge and calculate burst/drop score
merged = window_df.merge(baseline_df, on='word_text', how='outer').fillna(0)

# Normalize by number of days
window_days = 11
baseline_days = 323  # ~365 - 42 (approximate)

merged['window_daily_rate'] = merged['window_count'] / window_days
merged['baseline_daily_rate'] = merged['baseline_count'] / baseline_days

# Change score = (window_rate - baseline_rate) / baseline_rate
merged['change_score'] = ((merged['window_daily_rate'] - merged['baseline_daily_rate']) 
                         / (merged['baseline_daily_rate'] + 0.1))  # +0.1 to avoid div by zero

# Separate spikes and drops
spikes = merged[
    (merged['change_score'] > 2.0) &  # 200%+ increase
    (merged['window_count'] > 10)
].sort_values('change_score', ascending=False)

drops = merged[
    (merged['change_score'] < -0.5) &  # 50%+ decrease
    (merged['baseline_count'] > 100) &  # Was common before
    (merged['baseline_daily_rate'] > 1.0)  # At least daily in baseline
].sort_values('change_score', ascending=True)

# SPIKES
print(f"\n{'='*70}")
print(f"TOP SPIKING WORDS (Aug 15-25, 2024)")
print(f"{'='*70}\n")

print(f"{'Word':<20} {'Window':<10} {'Baseline':<10} {'Change':<12}")
print(f"{'-'*20} {'-'*10} {'-'*10} {'-'*12}")

for _, row in spikes.head(30).iterrows():
    print(f"{row['word_text']:<20} {row['window_count']:>9.0f} "
          f"{row['baseline_daily_rate']*window_days:>9.1f} "
          f"{row['change_score']:>11.1f}x")

# DROPS
print(f"\n{'='*70}")
print(f"TOP DROPPING WORDS (Aug 15-25, 2024)")
print(f"{'='*70}\n")

print(f"{'Word':<20} {'Window':<10} {'Baseline':<10} {'Drop':<12}")
print(f"{'-'*20} {'-'*10} {'-'*10} {'-'*12}")

for _, row in drops.head(30).iterrows():
    print(f"{row['word_text']:<20} {row['window_count']:>9.0f} "
          f"{row['baseline_daily_rate']*window_days:>9.1f} "
          f"{row['change_score']:>11.1f}x")

# Save results
spikes.to_csv('data/word_bursts_aug15_25.csv', index=False)
drops.to_csv('data/word_drops_aug15_25.csv', index=False)

print(f"\n✓ Saved:")
print(f"  - data/word_bursts_aug15_25.csv")
print(f"  - data/word_drops_aug15_25.csv")

pg_conn.close()