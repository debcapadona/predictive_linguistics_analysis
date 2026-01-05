"""
Cross-platform validation: Compare HN vs Reddit signals
"""
import pandas as pd
import numpy as np
import sqlite3
import psycopg2
from scipy.stats import ttest_ind
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

print("="*70)
print("CROSS-PLATFORM VALIDATION: HN vs REDDIT")
print("="*70)

# Test cases
test_cases = [
    {
        'name': 'Reddit API Blackout',
        'date': '2024-06-12',
        'window_days': 3
    },
    {
        'name': 'Trump Assassination Attempt',
        'date': '2024-07-13',
        'window_days': 3
    },
    {
        'name': 'CrowdStrike Outage',
        'date': '2024-07-19',
        'window_days': 3
    },
    {
        'name': 'Election Day',
        'date': '2024-11-05',
        'window_days': 3
    }
]

dimensions = [
    'emotional_valence_shift',
    'temporal_bleed',
    'certainty_collapse',
    'time_compression',
    'agency_reversal',
    'metaphor_cluster_density',
    'novel_meme_explosion',
    'sacred_profane_ratio',
    'pronoun_flip'
]

# Connect to databases
print("\nConnecting to databases...")
reddit_conn = sqlite3.connect('data/reddit_snapshot_dec29.db')
pg_conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='linguistic_predictor_v2',
    user='analyzer',
    password='dev_password_change_in_prod'
)

results = []

for test_case in test_cases:
    event_name = test_case['name']
    event_date = datetime.strptime(test_case['date'], '%Y-%m-%d')
    window = test_case['window_days']
    
    start_date = event_date - timedelta(days=window)
    end_date = event_date + timedelta(days=window)
    
    print(f"\n{'='*70}")
    print(f"Event: {event_name} ({event_date.date()})")
    print(f"Window: {start_date.date()} to {end_date.date()}")
    print(f"{'='*70}")
    
    for dimension in dimensions:
        # Get HN scores
        hn_query = f"""
            SELECT AVG(b.score) as avg_score, COUNT(*) as count
            FROM bert_{dimension} b
            JOIN stories s ON b.story_id = s.id
            WHERE DATE(s.created_at) BETWEEN %s AND %s
        """
        
        hn_df = pd.read_sql(hn_query, pg_conn, params=(start_date, end_date))
        hn_score = hn_df['avg_score'].values[0] if hn_df['avg_score'].values[0] else 0
        hn_count = hn_df['count'].values[0]
        
        # Get Reddit scores
        reddit_query = f"""
            SELECT AVG(b.score) as avg_score, COUNT(*) as count
            FROM reddit_bert_{dimension} b
            JOIN reddit_comments c ON b.comment_id = c.id
            WHERE DATE(datetime(c.created_utc, 'unixepoch')) BETWEEN ? AND ?
        """
        
        reddit_df = pd.read_sql(reddit_query, reddit_conn, 
                               params=(start_date.strftime('%Y-%m-%d'), 
                                      end_date.strftime('%Y-%m-%d')))
        reddit_score = reddit_df['avg_score'].values[0] if reddit_df['avg_score'].values[0] else 0
        reddit_count = reddit_df['count'].values[0]
        
        # Calculate difference
        diff = abs(hn_score - reddit_score)
        pct_diff = (diff / hn_score * 100) if hn_score > 0 else 0
        
        results.append({
            'event': event_name,
            'dimension': dimension,
            'hn_score': hn_score,
            'hn_count': hn_count,
            'reddit_score': reddit_score,
            'reddit_count': reddit_count,
            'difference': diff,
            'pct_difference': pct_diff
        })
        
        print(f"  {dimension:30s} | HN: {hn_score:.4f} ({hn_count:5d}) | Reddit: {reddit_score:.4f} ({reddit_count:6d}) | Diff: {pct_diff:5.1f}%")

# Save results
results_df = pd.DataFrame(results)
results_df.to_csv('data/hn_reddit_comparison.csv', index=False)
print(f"\n✓ Saved results to data/hn_reddit_comparison.csv")

# Create comparison heatmap
print("\nCreating comparison visualization...")

fig, axes = plt.subplots(2, 2, figsize=(16, 12))
axes = axes.flatten()

for idx, test_case in enumerate(test_cases):
    event_data = results_df[results_df['event'] == test_case['name']]
    
    # Pivot for heatmap
    pivot = event_data.pivot_table(
        values=['hn_score', 'reddit_score'],
        index='dimension',
        aggfunc='first'
    )
    
    sns.heatmap(pivot, annot=True, fmt='.3f', cmap='RdYlBu_r',
               ax=axes[idx], cbar_kws={'label': 'Score'})
    axes[idx].set_title(f"{test_case['name']}\n{test_case['date']}", 
                       fontsize=12, fontweight='bold')
    axes[idx].set_xlabel('')
    axes[idx].set_ylabel('')

plt.tight_layout()
plt.savefig('visualizations/hn_reddit_comparison_heatmap.png', dpi=300, bbox_inches='tight')
print("  ✓ Saved: visualizations/hn_reddit_comparison_heatmap.png")

reddit_conn.close()
pg_conn.close()

print("\n" + "="*70)
print("CROSS-PLATFORM VALIDATION COMPLETE")
print("="*70)
print(f"\nAnalyzed {len(test_cases)} events across {len(dimensions)} dimensions")
print("Results saved to data/hn_reddit_comparison.csv")