"""
Calculate Event Coherence Index
Implements Yuri's methodology:
1. First derivative (rate of change)
2. Second derivative (curvature)
3. Cross-dimensional synchronization
4. Pre-post asymmetry
"""
import pandas as pd
import psycopg2
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

print("="*70)
print("EVENT COHERENCE INDEX CALCULATION")
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

# Load all dimension data
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
    df = df.set_index('date')
    all_data[dim] = df

# Create unified dataframe
combined = pd.DataFrame()
for dim in dimensions:
    combined[dim] = all_data[dim]['avg_score']

print(f"  ✓ Loaded {len(dimensions)} dimensions, {len(combined)} days")

# 1. Calculate first derivative (Δ)
print("\n1. Calculating first derivative (rate of change)...")
delta = combined.diff()
delta.columns = [f'{col}_delta' for col in delta.columns]

# 2. Calculate second derivative (Δ²) - curvature
print("2. Calculating second derivative (curvature)...")
delta2 = delta.diff()
delta2.columns = [f'{col}_delta2' for col in combined.columns]

# 3. Calculate cross-dimensional synchronization
print("3. Calculating cross-dimensional synchronization...")

# For each day, count how many dimensions show significant change
threshold_multiplier = 1.5  # k * std threshold

coherence_scores = []

for date_idx in range(len(combined)):
    date = combined.index[date_idx]
    
    # Count dimensions with significant Δ
    significant_deltas = 0
    for dim in dimensions:
        delta_col = f'{dim}_delta'
        if delta_col in delta.columns:
            val = delta.iloc[date_idx][delta_col]
            if pd.notna(val):
                std = delta[delta_col].std()
                if abs(val) > threshold_multiplier * std:
                    significant_deltas += 1
    
    # Count dimensions with significant Δ²
    significant_delta2s = 0
    for dim in dimensions:
        delta2_col = f'{dim}_delta2'
        if delta2_col in delta2.columns:
            val = delta2.iloc[date_idx][delta2_col]
            if pd.notna(val):
                std = delta2[delta2_col].std()
                if abs(val) > threshold_multiplier * std:
                    significant_delta2s += 1
    
    # Coherence score = average of both
    coherence = (significant_deltas + significant_delta2s) / (2 * len(dimensions))
    coherence_scores.append(coherence)

coherence_series = pd.Series(coherence_scores, index=combined.index)

# 4. Calculate pre-post asymmetry
print("4. Calculating pre-post asymmetry ratio...")

window = 5  # ±5 days
asymmetry_scores = []

for date_idx in range(window, len(combined) - window):
    date = combined.index[date_idx]
    
    # Get window around this date
    pre_window = combined.iloc[date_idx-window:date_idx]
    spike = combined.iloc[date_idx]
    post_window = combined.iloc[date_idx+1:date_idx+window+1]
    
    # Calculate average dip depth pre/post
    pre_dips = []
    post_dips = []
    spike_heights = []
    
    for dim in dimensions:
        pre_mean = pre_window[dim].mean()
        spike_val = spike[dim]
        post_mean = post_window[dim].mean()
        
        pre_dip = max(0, pre_mean - spike_val)  # How much below spike
        post_dip = max(0, post_mean - spike_val)
        spike_height = abs(spike_val - combined[dim].mean())
        
        if spike_height > 0:
            asymmetry = (pre_dip + post_dip) / spike_height
            asymmetry_scores.append(asymmetry)

# Pad with NaN for edges
full_asymmetry = [np.nan] * window
full_asymmetry.extend([np.mean(asymmetry_scores[i:i+len(dimensions)]) 
                       for i in range(0, len(asymmetry_scores), len(dimensions))])
full_asymmetry.extend([np.nan] * window)

asymmetry_series = pd.Series(full_asymmetry[:len(combined)], index=combined.index)

# 5. Combine into Event Coherence Index
print("5. Creating composite Event Coherence Index...")

# Normalize coherence (already 0-1)
# Normalize asymmetry to 0-1
asymmetry_norm = (asymmetry_series - asymmetry_series.min()) / (asymmetry_series.max() - asymmetry_series.min())

# Event Coherence Index = weighted combination
event_coherence = 0.7 * coherence_series + 0.3 * asymmetry_norm

# Smooth with 3-day window
event_coherence_smoothed = event_coherence.rolling(window=3, center=True).mean()

print(f"  ✓ Event Coherence Index calculated")

# Save results
results_df = pd.DataFrame({
    'date': combined.index,
    'coherence_score': coherence_series.values,
    'asymmetry_score': asymmetry_series.values,
    'event_coherence_index': event_coherence_smoothed.values
})

results_df.to_csv('data/event_coherence_index.csv', index=False)
print("\n✓ Saved: data/event_coherence_index.csv")

# Visualize
print("\nCreating visualization...")

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(24, 14), sharex=True)

# Top: All dimensions
colors = plt.cm.tab10(np.linspace(0, 1, 9))
for idx, dim in enumerate(dimensions):
    ax1.plot(combined.index, combined[dim].rolling(7, center=True).mean(),
            linewidth=1.5, alpha=0.6, color=colors[idx],
            label=dim.replace('_', ' ').title())

ax1.set_ylabel('Dimension Score (7-day MA)', fontsize=12, fontweight='bold')
ax1.set_title('Linguistic Dimensions + Event Coherence Index',
             fontsize=16, fontweight='bold', pad=15)
ax1.legend(loc='upper left', fontsize=9, ncol=3)
ax1.grid(True, alpha=0.3)

# Bottom: Event Coherence Index
ax2.plot(combined.index, event_coherence_smoothed,
        linewidth=3, color='red', label='Event Coherence Index')
ax2.axhline(0.5, color='orange', linestyle='--', alpha=0.7,
           label='High Coherence Threshold')
ax2.fill_between(combined.index, 0, event_coherence_smoothed,
                where=(event_coherence_smoothed > 0.5),
                alpha=0.3, color='red', label='Event Regime')

ax2.set_xlabel('Date (2024)', fontsize=12, fontweight='bold')
ax2.set_ylabel('Event Coherence\n(0=noise, 1=event)', fontsize=12, fontweight='bold')
ax2.set_ylim(0, 1)
ax2.legend(loc='upper left', fontsize=10)
ax2.grid(True, alpha=0.3)

# Format x-axis
ax2.xaxis.set_major_locator(mdates.DayLocator(bymonthday=[1, 10, 20]))
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b-%d'))
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=90, ha='center', fontsize=10)

plt.tight_layout()
plt.savefig('visualizations/event_coherence_index.png', dpi=300, bbox_inches='tight')
print("✓ Saved: visualizations/event_coherence_index.png")

# Print top coherence dates
print("\nTop 20 Event Coherence dates:")
top_dates = results_df.nlargest(20, 'event_coherence_index')
print(top_dates[['date', 'event_coherence_index']].to_string(index=False))

pg_conn.close()

print("\n" + "="*70)
print("EVENT COHERENCE INDEX COMPLETE")
print("="*70)
print("\nOpen with: xdg-open visualizations/event_coherence_index.png")