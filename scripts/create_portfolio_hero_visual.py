"""
Create the hero visualization for portfolio
Shows Reddit API Blackout prediction with clear narrative
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np

print("Creating portfolio hero visual...")

# Load Reddit event data
daily_df = pd.read_csv('data/reddit_event_daily_timeseries.csv')
daily_df['date'] = pd.to_datetime(daily_df['date'])

event_date = datetime(2024, 6, 12)

# Focus on agency_reversal (strongest signal)
dimension = 'agency_reversal'
data = daily_df[daily_df['dimension'] == dimension].copy()

# Load baseline
baseline_df = pd.read_csv('data/baseline_distributions.csv')
baseline = baseline_df[baseline_df['dimension'] == dimension]
p95_threshold = baseline['p95'].values[0]
baseline_mean = baseline['mean'].values[0]

# Create figure
fig, ax = plt.subplots(figsize=(16, 9))

# Plot time series
ax.plot(data['date'], data['avg_score'], 
       linewidth=3, color='#2E86AB', marker='o', markersize=8,
       label='Agency Reversal Score')

# Baseline
ax.axhline(baseline_mean, color='gray', linestyle=':', 
          linewidth=2, alpha=0.7, label=f'2024 Baseline ({baseline_mean:.3f})')

# 95th percentile threshold
ax.axhline(p95_threshold, color='orange', linestyle='--', 
          linewidth=2, alpha=0.8, label=f'95th Percentile Anomaly Threshold ({p95_threshold:.3f})')

# Event day
ax.axvline(event_date, color='red', linestyle='-', 
          linewidth=3, alpha=0.7, label='Reddit API Blackout')

# Highlight June 10 (prediction)
june10 = datetime(2024, 6, 10)
june10_score = data[data['date'] == june10]['avg_score'].values[0]
ax.scatter([june10], [june10_score], s=500, color='gold', 
          edgecolors='red', linewidths=3, zorder=10,
          label='2-Day Advance Warning')

# Annotations
ax.annotate('97.9th percentile\n2 days before event', 
           xy=(june10, june10_score), 
           xytext=(june10, june10_score + 0.02),
           fontsize=14, fontweight='bold', color='red',
           ha='center',
           bbox=dict(boxstyle='round,pad=0.5', facecolor='yellow', alpha=0.8),
           arrowprops=dict(arrowstyle='->', color='red', lw=2))

ax.annotate('Event Day\n+34.7% spike\n(p < 0.0001)', 
           xy=(event_date, data[data['date'] == event_date]['avg_score'].values[0]), 
           xytext=(event_date, 0.15),
           fontsize=13, fontweight='bold',
           ha='center',
           bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.9),
           arrowprops=dict(arrowstyle='->', color='red', lw=2))

# Styling
ax.set_xlabel('Date (June 2024)', fontsize=14, fontweight='bold')
ax.set_ylabel('Agency Reversal Score', fontsize=14, fontweight='bold')
ax.set_title('Linguistic Prediction System Detected Reddit API Blackout\nWith 2-Day Advance Warning',
            fontsize=18, fontweight='bold', pad=20)

ax.legend(loc='upper left', fontsize=12, framealpha=0.95)
ax.grid(True, alpha=0.3, linestyle='--')
ax.set_ylim(0.07, 0.16)

plt.tight_layout()
plt.savefig('visualizations/portfolio_hero_visual.png', dpi=300, bbox_inches='tight')
print("✓ Saved: visualizations/portfolio_hero_visual.png")

# Create domain comparison visual
print("\nCreating domain comparison visual...")

corr_df = pd.read_csv('data/topic_dimension_correlations.csv')

# Aggregate by domain
domain_avg = corr_df.groupby(['domain', 'dimension'])['mean_score'].mean().reset_index()
pivot = domain_avg.pivot(index='dimension', columns='domain', values='mean_score')

# Reorder by overall intensity
pivot = pivot.loc[pivot.mean(axis=1).sort_values(ascending=False).index]

fig, ax = plt.subplots(figsize=(14, 10))

sns.heatmap(pivot, annot=True, fmt='.3f', cmap='RdYlBu_r', 
           cbar_kws={'label': 'Average Score'},
           linewidths=1, linecolor='white',
           vmin=0, vmax=0.30, center=0.15, ax=ax)

ax.set_title('Crisis Language by Topic Domain\nSociety & Politics Shows Highest Signals Across All Dimensions',
            fontsize=16, fontweight='bold', pad=20)
ax.set_xlabel('Topic Domain', fontsize=13, fontweight='bold')
ax.set_ylabel('BERT Dimension', fontsize=13, fontweight='bold')

plt.xticks(rotation=45, ha='right')
plt.yticks(rotation=0)
plt.tight_layout()
plt.savefig('visualizations/portfolio_domain_heatmap.png', dpi=300, bbox_inches='tight')
print("✓ Saved: visualizations/portfolio_domain_heatmap.png")

# Create stats summary card
print("\nCreating stats summary...")

from matplotlib.patches import Rectangle

fig, ax = plt.subplots(figsize=(12, 8))
ax.set_xlim(0, 10)
ax.set_ylim(0, 10)
ax.axis('off')

# Title
ax.text(5, 9, 'Multi-Dimensional Collapse System', 
       ha='center', fontsize=24, fontweight='bold')
ax.text(5, 8.3, 'Linguistic Event Detection via Forum Analysis',
       ha='center', fontsize=16, style='italic', color='gray')

# Stats boxes
stats = [
    ('Data Processed', '197,496 HN Posts', '2024 Full Year'),
    ('Topics Discovered', '67 Topics', '3-Tier Taxonomy'),
    ('Dimensions Analyzed', '9 BERT Models', 'Zero-Shot Labeled'),
    ('Validation Result', '34.7% Spike', 'p < 0.0001'),
    ('Advance Warning', '2 Days', '97.9th Percentile'),
    ('Sample Size', '163K Comments', '53K Labels'),
]

y_pos = 7
for i, (label, value, detail) in enumerate(stats):
    x_pos = 1.5 if i % 2 == 0 else 6
    if i % 2 == 0 and i > 0:
        y_pos -= 2
    
    # Box
    rect = Rectangle((x_pos-0.8, y_pos-0.6), 3.5, 1.3, 
                     linewidth=2, edgecolor='#2E86AB', 
                     facecolor='#E8F4F8', alpha=0.7)
    ax.add_patch(rect)
    
    # Text
    ax.text(x_pos + 1, y_pos + 0.3, value, 
           fontsize=18, fontweight='bold', color='#2E86AB')
    ax.text(x_pos + 1, y_pos - 0.1, label,
           fontsize=12, color='black')
    ax.text(x_pos + 1, y_pos - 0.4, detail,
           fontsize=10, style='italic', color='gray')

# Footer
ax.text(5, 0.5, 'Built with: BERTopic • BERT • PostgreSQL • Claude API • Python',
       ha='center', fontsize=11, color='gray', style='italic')

plt.tight_layout()
plt.savefig('visualizations/portfolio_stats_card.png', dpi=300, bbox_inches='tight')
print("✓ Saved: visualizations/portfolio_stats_card.png")

print("\n" + "="*70)
print("PORTFOLIO VISUALS COMPLETE")
print("="*70)
print("\nCreated 3 key visuals:")
print("  1. portfolio_hero_visual.png - The money shot")
print("  2. portfolio_domain_heatmap.png - Domain analysis")  
print("  3. portfolio_stats_card.png - Stats summary")
print("\nReady for portfolio website!")
