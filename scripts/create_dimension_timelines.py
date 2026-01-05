"""
Create full-year timelines for each dimension
Shows daily averages with event markers
"""
import pandas as pd
import numpy as np
import sqlite3
import psycopg2
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

print("="*70)
print("CREATING DIMENSION TIMELINES")
print("="*70)

# Event markers
events = [
    {'name': 'Reddit API\nBlackout', 'date': '2024-06-12', 'color': '#E63946'},
    {'name': 'Trump\nAssassination', 'date': '2024-07-13', 'color': '#F77F00'},
    {'name': 'CrowdStrike\nOutage', 'date': '2024-07-19', 'color': '#FCBF49'},
    {'name': 'Election\nDay', 'date': '2024-11-05', 'color': '#06BA63'}
]

dimensions = [
    'emotional_valence_shift', 'temporal_bleed', 'certainty_collapse',
    'time_compression', 'agency_reversal', 'metaphor_cluster_density',
    'novel_meme_explosion', 'sacred_profane_ratio', 'pronoun_flip'
]

# Connect to databases
print("\nConnecting to databases...")
reddit_conn = sqlite3.connect('data/reddit_snapshot_dec29.db')
pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)

# Create timeline for each dimension
for dimension in dimensions:
    print(f"\nProcessing: {dimension}")
    
    # Get HN daily averages
    hn_query = f"""
        SELECT 
            DATE(s.created_at) as date,
            AVG(b.score) as avg_score,
            COUNT(*) as count
        FROM bert_{dimension} b
        JOIN stories s ON b.story_id = s.id
        WHERE DATE(s.created_at) BETWEEN '2024-01-01' AND '2024-11-30'
        GROUP BY DATE(s.created_at)
        ORDER BY DATE(s.created_at)
    """
    hn_df = pd.read_sql(hn_query, pg_conn)
    hn_df['date'] = pd.to_datetime(hn_df['date'])
    hn_df['platform'] = 'HN'
    
    # Get Reddit daily averages
    reddit_query = f"""
        SELECT 
            DATE(datetime(c.created_utc, 'unixepoch')) as date,
            AVG(b.score) as avg_score,
            COUNT(*) as count
        FROM reddit_bert_{dimension} b
        JOIN reddit_comments c ON b.comment_id = c.id
        WHERE DATE(datetime(c.created_utc, 'unixepoch')) BETWEEN '2024-01-01' AND '2024-11-30'
        GROUP BY DATE(datetime(c.created_utc, 'unixepoch'))
        ORDER BY DATE(datetime(c.created_utc, 'unixepoch'))
    """
    reddit_df = pd.read_sql(reddit_query, reddit_conn)
    reddit_df['date'] = pd.to_datetime(reddit_df['date'])
    reddit_df['platform'] = 'Reddit'
    
    # Calculate baselines and percentiles
    hn_baseline = hn_df['avg_score'].mean()
    hn_95th = hn_df['avg_score'].quantile(0.95)
    
    reddit_baseline = reddit_df['avg_score'].mean()
    reddit_95th = reddit_df['avg_score'].quantile(0.95)
    
    # Create figure
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 10), sharex=True)
    
    # HN timeline
    ax1.plot(hn_df['date'], hn_df['avg_score'], 
            linewidth=1.5, color='#2E86AB', alpha=0.7, label='Daily Average')
    ax1.axhline(hn_baseline, color='gray', linestyle=':', 
               linewidth=2, alpha=0.7, label=f'Baseline ({hn_baseline:.3f})')
    ax1.axhline(hn_95th, color='orange', linestyle='--', 
               linewidth=2, alpha=0.7, label=f'95th Percentile ({hn_95th:.3f})')
    
    # Add event markers
    for event in events:
        event_date = pd.to_datetime(event['date'])
        if event_date in hn_df['date'].values:
            score = hn_df[hn_df['date'] == event_date]['avg_score'].values[0]
            ax1.axvline(event_date, color=event['color'], linestyle='-', 
                       linewidth=2, alpha=0.6)
            ax1.text(event_date, ax1.get_ylim()[1] * 0.95, event['name'], 
                    rotation=90, ha='right', va='top', fontsize=9,
                    bbox=dict(boxstyle='round', facecolor=event['color'], alpha=0.3))
    
    ax1.set_ylabel('Score', fontsize=12, fontweight='bold')
    ax1.set_title(f"Hacker News - {dimension.replace('_', ' ').title()}", 
                 fontsize=14, fontweight='bold')
    ax1.legend(loc='upper left', fontsize=10)
    ax1.grid(True, alpha=0.3)
    
    # Reddit timeline
    ax2.plot(reddit_df['date'], reddit_df['avg_score'], 
            linewidth=1.5, color='#A23B72', alpha=0.7, label='Daily Average')
    ax2.axhline(reddit_baseline, color='gray', linestyle=':', 
               linewidth=2, alpha=0.7, label=f'Baseline ({reddit_baseline:.3f})')
    ax2.axhline(reddit_95th, color='orange', linestyle='--', 
               linewidth=2, alpha=0.7, label=f'95th Percentile ({reddit_95th:.3f})')
    
    # Add event markers
    for event in events:
        event_date = pd.to_datetime(event['date'])
        if event_date in reddit_df['date'].values:
            score = reddit_df[reddit_df['date'] == event_date]['avg_score'].values[0]
            ax2.axvline(event_date, color=event['color'], linestyle='-', 
                       linewidth=2, alpha=0.6)
            ax2.text(event_date, ax2.get_ylim()[1] * 0.95, event['name'], 
                    rotation=90, ha='right', va='top', fontsize=9,
                    bbox=dict(boxstyle='round', facecolor=event['color'], alpha=0.3))
    
    ax2.set_xlabel('Date (2024)', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Score', fontsize=12, fontweight='bold')
    ax2.set_title(f"Reddit - {dimension.replace('_', ' ').title()}", 
                 fontsize=14, fontweight='bold')
    ax2.legend(loc='upper left', fontsize=10)
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'visualizations/timeline_{dimension}.png', dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: visualizations/timeline_{dimension}.png")

reddit_conn.close()
pg_conn.close()

print("\n" + "="*70)
print("TIMELINES COMPLETE")
print("="*70)
print("\nCreated 9 timeline visualizations showing:")
print("  - Daily averages for full year")
print("  - Baseline (mean)")
print("  - 95th percentile threshold")
print("  - Event markers")
print("\nCheck visualizations/ folder!")