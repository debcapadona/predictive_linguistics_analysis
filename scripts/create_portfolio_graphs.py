"""
Create key portfolio visualizations
1. All 9 dimensions on one timeline
2. Topic clustering over time (full year)
"""
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np

print("="*70)
print("CREATING PORTFOLIO GRAPHS")
print("="*70)

pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)

dimensions = [
    'emotional_valence_shift', 'temporal_bleed', 'certainty_collapse',
    'time_compression', 'agency_reversal', 'metaphor_cluster_density',
    'novel_meme_explosion', 'sacred_profane_ratio', 'pronoun_flip'
]

# Event markers
events = [
    {'name': 'Reddit API\nBlackout', 'date': '2024-06-12', 'color': '#E63946'},
    {'name': 'Trump\nAssassination', 'date': '2024-07-13', 'color': '#F77F00'},
]

# ============================================================================
# GRAPH 1: All 9 dimensions on one timeline
# ============================================================================
print("\n1. Creating all-dimensions timeline...")

fig, ax = plt.subplots(figsize=(20, 10))

colors = plt.cm.tab10(np.linspace(0, 1, 9))

for idx, dim in enumerate(dimensions):
    query = f"""
        SELECT 
            DATE(s.created_at) as date,
            AVG(b.score) as avg_score
        FROM bert_{dim} b
        JOIN stories s ON b.story_id = s.id
        WHERE DATE(s.created_at) BETWEEN '2024-01-01' AND '2024-11-30'
        GROUP BY DATE(s.created_at)
        ORDER BY DATE(s.created_at)
    """
    df = pd.read_sql(query, pg_conn)
    df['date'] = pd.to_datetime(df['date'])
    
    # Plot with smoothing
    ax.plot(df['date'], df['avg_score'].rolling(window=7, center=True).mean(),
           linewidth=2, alpha=0.8, color=colors[idx],
           label=dim.replace('_', ' ').title())

# Add event markers
for event in events:
    event_date = pd.to_datetime(event['date'])
    ax.axvline(event_date, color=event['color'], linestyle='--', 
              linewidth=2, alpha=0.7)
    ax.text(event_date, ax.get_ylim()[1] * 0.95, event['name'],
           rotation=0, ha='center', va='top', fontsize=11, fontweight='bold',
           bbox=dict(boxstyle='round', facecolor=event['color'], alpha=0.3))

ax.set_xlabel('Date (2024)', fontsize=14, fontweight='bold')
ax.set_ylabel('Dimension Score (7-day moving average)', fontsize=14, fontweight='bold')
ax.set_title('Linguistic Dimensions Timeline - Hacker News 2024',
            fontsize=18, fontweight='bold', pad=20)
ax.legend(loc='upper left', fontsize=10, ncol=2)
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('visualizations/all_dimensions_timeline.png', dpi=300, bbox_inches='tight')
print("  ✓ Saved: visualizations/all_dimensions_timeline.png")

# ============================================================================
# GRAPH 2: Topic clustering over time
# ============================================================================
print("\n2. Creating topic clustering timeline...")

# Get daily topic distributions
query = """
    SELECT 
        DATE(s.created_at) as date,
        t1.topic_name as domain,
        COUNT(*) as comment_count
    FROM comment_labels cl
    JOIN stories s ON cl.comment_id::text = s.id
    JOIN topic_taxonomy t3 ON cl.topic_id = t3.id
    JOIN topic_taxonomy t2 ON t3.parent_id = t2.id
    JOIN topic_taxonomy t1 ON t2.parent_id = t1.id
    WHERE DATE(s.created_at) BETWEEN '2024-01-01' AND '2024-11-30'
    AND cl.label_type = 'topic'
    AND t1.tier = 1
    GROUP BY DATE(s.created_at), t1.topic_name
    ORDER BY date, comment_count DESC
"""

df = pd.read_sql(query, pg_conn)
df['date'] = pd.to_datetime(df['date'])

# Pivot for stacked area chart
pivot = df.pivot_table(index='date', columns='domain', values='comment_count', fill_value=0)

# Create stacked area chart
fig, ax = plt.subplots(figsize=(20, 10))

ax.stackplot(pivot.index, 
            *[pivot[col] for col in pivot.columns],
            labels=pivot.columns,
            alpha=0.8)

# Add event markers
for event in events:
    event_date = pd.to_datetime(event['date'])
    ax.axvline(event_date, color=event['color'], linestyle='--',
              linewidth=3, alpha=0.9, zorder=10)
    ax.text(event_date, ax.get_ylim()[1] * 0.95, event['name'],
           rotation=0, ha='center', va='top', fontsize=12, fontweight='bold',
           bbox=dict(boxstyle='round', facecolor=event['color'], alpha=0.5),
           zorder=11)

ax.set_xlabel('Date (2024)', fontsize=14, fontweight='bold')
ax.set_ylabel('Number of Comments by Domain', fontsize=14, fontweight='bold')
ax.set_title('Topic Domain Distribution Over Time - Hacker News 2024',
            fontsize=18, fontweight='bold', pad=20)
ax.legend(loc='upper left', fontsize=12)
ax.grid(True, alpha=0.3, axis='y')

plt.tight_layout()
plt.savefig('visualizations/topic_clustering_timeline.png', dpi=300, bbox_inches='tight')
print("  ✓ Saved: visualizations/topic_clustering_timeline.png")

pg_conn.close()

print("\n" + "="*70)
print("GRAPHS COMPLETE")
print("="*70)
print("\nCreated:")
print("  1. visualizations/all_dimensions_timeline.png")
print("  2. visualizations/topic_clustering_timeline.png")