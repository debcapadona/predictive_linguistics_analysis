"""
Create timeline showing all 9 dimensions + temporal score overlay
"""
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.dates as mdates

print("="*70)
print("CREATING TIMELINE WITH TEMPORAL SCORES")
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

colors = plt.cm.tab10(np.linspace(0, 1, 9))

# Load dimension data
print("\nLoading dimension data...")
all_data = {}
for dim in dimensions:
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
    all_data[dim] = df

# Load temporal scores
print("Loading temporal scores...")
temporal_query = """
    SELECT 
        date,
        AVG(avg_temporal_score) as avg_temporal,
        SUM(with_temporal_context) as context_count
    FROM word_temporal_scores
    WHERE date BETWEEN '2024-01-01' AND '2024-11-30'
    GROUP BY date
    ORDER BY date
"""
temporal_df = pd.read_sql(temporal_query, pg_conn)
temporal_df['date'] = pd.to_datetime(temporal_df['date'])
temporal_df['smoothed'] = temporal_df['avg_temporal'].rolling(window=7, center=True).mean()

print(f"  ✓ Loaded data for {len(all_data)} dimensions + temporal")

# Create figure with dual y-axis
fig, ax1 = plt.subplots(figsize=(24, 12))

# Plot dimensions on left axis
for idx, dim in enumerate(dimensions):
    df = all_data[dim]
    ax1.plot(df['date'], df['smoothed'],
            linewidth=2, alpha=0.7, color=colors[idx],
            label=dim.replace('_', ' ').title())

ax1.set_xlabel('Date (2024) - Markers on 1st, 10th, 20th', fontsize=14, fontweight='bold')
ax1.set_ylabel('Dimension Score (7-day MA)', fontsize=14, fontweight='bold', color='black')
ax1.tick_params(axis='y', labelcolor='black')

# Create second y-axis for temporal score
ax2 = ax1.twinx()
ax2.plot(temporal_df['date'], temporal_df['smoothed'],
        linewidth=3, alpha=0.9, color='red', linestyle='--',
        label='Temporal Urgency Score', zorder=10)

ax2.set_ylabel('Temporal Urgency Score (0=distant, 1=immediate)', 
              fontsize=14, fontweight='bold', color='red')
ax2.tick_params(axis='y', labelcolor='red')
ax2.set_ylim(0, 1)

# Format x-axis
ax1.xaxis.set_major_locator(mdates.DayLocator(bymonthday=[1, 10, 20]))
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b-%d'))
plt.setp(ax1.xaxis.get_majorticklabels(), rotation=90, ha='center', fontsize=10)

# Legends
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, 
          loc='upper left', fontsize=11, ncol=3)

# Grid
ax1.grid(True, alpha=0.3, which='major', axis='x', linestyle='-', linewidth=0.8)
ax1.grid(True, alpha=0.2, which='major', axis='y')

plt.title('Linguistic Dimensions + Temporal Urgency - Full 2024 (Hacker News)',
         fontsize=18, fontweight='bold', pad=20)

plt.tight_layout()
plt.savefig('visualizations/timeline_with_temporal.png', dpi=300, bbox_inches='tight')
print("\n✓ Saved: visualizations/timeline_with_temporal.png")

pg_conn.close()

print("\n" + "="*70)
print("VISUALIZATION COMPLETE")
print("="*70)
print("\nRed dashed line = Temporal urgency (higher = more immediate language)")
print("Colored lines = 9 BERT dimensions")
print("\nOpen with: xdg-open visualizations/timeline_with_temporal.png")