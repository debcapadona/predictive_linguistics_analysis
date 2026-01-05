"""
Full year timeline with 1st, 10th, 20th date markers
"""
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.dates as mdates

print("Creating detailed timeline with date markers...")

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

colors = plt.cm.tab10(np.linspace(0, 1, 9))

# Create figure
fig, ax = plt.subplots(figsize=(24, 12))

# Load and plot all dimensions
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
    df['smoothed'] = df['avg_score'].rolling(window=7, center=True).mean()
    
    ax.plot(df['date'], df['smoothed'],
           linewidth=2.5, alpha=0.8, color=colors[idx],
           label=dim.replace('_', ' ').title())

# Set up x-axis with 1st, 10th, 20th of each month
ax.xaxis.set_major_locator(mdates.DayLocator(bymonthday=[1, 10, 20]))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b-%d'))

# Rotate labels vertically
plt.setp(ax.xaxis.get_majorticklabels(), rotation=90, ha='center', fontsize=10)

# Grid on major dates
ax.grid(True, alpha=0.3, which='major', axis='x', linestyle='-', linewidth=0.8)
ax.grid(True, alpha=0.2, which='major', axis='y')

ax.set_xlabel('Date (2024) - Markers on 1st, 10th, 20th of each month', 
             fontsize=14, fontweight='bold')
ax.set_ylabel('Dimension Score (7-day moving average)', 
             fontsize=14, fontweight='bold')
ax.set_title('Linguistic Dimensions Timeline - Full 2024 (Hacker News)',
            fontsize=18, fontweight='bold', pad=20)
ax.legend(loc='upper left', fontsize=11, ncol=3)

plt.tight_layout()
plt.savefig('visualizations/full_timeline_detailed.png', dpi=300, bbox_inches='tight')
print("✓ Saved: visualizations/full_timeline_detailed.png")

pg_conn.close()

print("\nOpen with: xdg-open visualizations/full_timeline_detailed.png")