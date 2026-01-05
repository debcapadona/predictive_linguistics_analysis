"""
Cross-platform event validation
Compare event spikes vs baseline for BOTH platforms
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
print("CROSS-PLATFORM EVENT VALIDATION")
print("="*70)

# Test cases
test_cases = [
    {'name': 'Reddit API Blackout', 'date': '2024-06-12', 'window': 3},
    {'name': 'Trump Assassination', 'date': '2024-07-13', 'window': 3},
    {'name': 'CrowdStrike Outage', 'date': '2024-07-19', 'window': 3},
    {'name': 'Election Day', 'date': '2024-11-05', 'window': 3}
]

dimensions = [
    'emotional_valence_shift', 'temporal_bleed', 'certainty_collapse',
    'time_compression', 'agency_reversal', 'metaphor_cluster_density',
    'novel_meme_explosion', 'sacred_profane_ratio', 'pronoun_flip'
]

# Connect to databases
reddit_conn = sqlite3.connect('data/reddit_snapshot_dec29.db')
pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)

results = []

# Calculate baselines (full year, excluding event windows)
print("\nCalculating baselines...")

# HN baseline
hn_baselines = {}
for dim in dimensions:
    query = f"""
        SELECT AVG(b.score) as baseline
        FROM bert_{dim} b
        JOIN stories s ON b.story_id = s.id
        WHERE DATE(s.created_at) BETWEEN '2024-01-01' AND '2024-11-30'
    """
    baseline = pd.read_sql(query, pg_conn)['baseline'].values[0]
    hn_baselines[dim] = baseline
    print(f"  HN {dim}: {baseline:.4f}")

# Reddit baseline
reddit_baselines = {}
for dim in dimensions:
    query = f"""
        SELECT AVG(b.score) as baseline
        FROM reddit_bert_{dim} b
        JOIN reddit_comments c ON b.comment_id = c.id
        WHERE DATE(datetime(c.created_utc, 'unixepoch')) BETWEEN '2024-01-01' AND '2024-11-30'
    """
    baseline = pd.read_sql(query, reddit_conn)['baseline'].values[0]
    reddit_baselines[dim] = baseline
    print(f"  Reddit {dim}: {baseline:.4f}")

# Analyze each event
for test_case in test_cases:
    event_name = test_case['name']
    event_date = datetime.strptime(test_case['date'], '%Y-%m-%d')
    window = test_case['window']
    
    start_date = event_date - timedelta(days=window)
    end_date = event_date + timedelta(days=window)
    
    print(f"\n{'='*70}")
    print(f"{event_name} - {event_date.date()}")
    print(f"{'='*70}")
    
    for dim in dimensions:
        # HN event score
        hn_query = f"""
            SELECT AVG(b.score) as event_score
            FROM bert_{dim} b
            JOIN stories s ON b.story_id = s.id
            WHERE DATE(s.created_at) BETWEEN %s AND %s
        """
        hn_event = pd.read_sql(hn_query, pg_conn, params=(start_date, end_date))['event_score'].values[0]
        hn_baseline = hn_baselines[dim]
        hn_change_pct = ((hn_event - hn_baseline) / hn_baseline * 100) if hn_baseline > 0 else 0
        hn_spike = "📈" if hn_change_pct > 10 else "📊"
        
        # Reddit event score
        reddit_query = f"""
            SELECT AVG(b.score) as event_score
            FROM reddit_bert_{dim} b
            JOIN reddit_comments c ON b.comment_id = c.id
            WHERE DATE(datetime(c.created_utc, 'unixepoch')) BETWEEN ? AND ?
        """
        reddit_event = pd.read_sql(reddit_query, reddit_conn, 
                                   params=(start_date.strftime('%Y-%m-%d'), 
                                          end_date.strftime('%Y-%m-%d')))['event_score'].values[0]
        reddit_baseline = reddit_baselines[dim]
        reddit_change_pct = ((reddit_event - reddit_baseline) / reddit_baseline * 100) if reddit_baseline > 0 else 0
        reddit_spike = "📈" if reddit_change_pct > 10 else "📊"
        
        # Both spiked?
        both_spike = "✅" if (hn_change_pct > 10 and reddit_change_pct > 10) else ""
        
        print(f"{dim:30s} | HN: {hn_spike} {hn_change_pct:+6.1f}% | Reddit: {reddit_spike} {reddit_change_pct:+6.1f}% {both_spike}")
        
        results.append({
            'event': event_name,
            'dimension': dim,
            'hn_baseline': hn_baseline,
            'hn_event': hn_event,
            'hn_change_pct': hn_change_pct,
            'reddit_baseline': reddit_baseline,
            'reddit_event': reddit_event,
            'reddit_change_pct': reddit_change_pct,
            'both_spike': (hn_change_pct > 10 and reddit_change_pct > 10)
        })

# Save results
results_df = pd.DataFrame(results)
results_df.to_csv('data/cross_platform_validation.csv', index=False)
print(f"\n✓ Saved: data/cross_platform_validation.csv")

# Summary stats
print("\n" + "="*70)
print("VALIDATION SUMMARY")
print("="*70)

for event in test_cases:
    event_results = results_df[results_df['event'] == event['name']]
    both_spike_count = event_results['both_spike'].sum()
    print(f"{event['name']:30s} | {both_spike_count}/9 dimensions spiked on BOTH platforms")

# Create validation visual
fig, axes = plt.subplots(2, 2, figsize=(18, 14))
axes = axes.flatten()

for idx, test_case in enumerate(test_cases):
    event_data = results_df[results_df['event'] == test_case['name']]
    
    ax = axes[idx]
    x = np.arange(len(dimensions))
    width = 0.35
    
    hn_changes = event_data['hn_change_pct'].values
    reddit_changes = event_data['reddit_change_pct'].values
    
    bars1 = ax.bar(x - width/2, hn_changes, width, label='HN', alpha=0.8, color='#2E86AB')
    bars2 = ax.bar(x + width/2, reddit_changes, width, label='Reddit', alpha=0.8, color='#A23B72')
    
    # Highlight >10% spikes
    ax.axhline(10, color='red', linestyle='--', alpha=0.5, linewidth=1)
    ax.axhline(-10, color='red', linestyle='--', alpha=0.5, linewidth=1)
    
    ax.set_ylabel('% Change vs Baseline', fontsize=11, fontweight='bold')
    ax.set_title(f"{test_case['name']}\n{test_case['date']}", fontsize=13, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([d.replace('_', '\n') for d in dimensions], rotation=45, ha='right', fontsize=9)
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    ax.axhline(0, color='black', linewidth=0.8)

plt.tight_layout()
plt.savefig('visualizations/cross_platform_validation.png', dpi=300, bbox_inches='tight')
print("✓ Saved: visualizations/cross_platform_validation.png")

reddit_conn.close()
pg_conn.close()

print("\nDone!")