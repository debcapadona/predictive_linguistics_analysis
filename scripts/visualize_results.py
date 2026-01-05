"""
Create visualizations of topic-dimension correlations
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

print("Loading results...")
df = pd.read_csv('data/topic_dimension_correlations.csv')

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 10)

# Create output directory
import os
os.makedirs('visualizations', exist_ok=True)

print("\n1. Creating domain × dimension heatmap...")

# Aggregate by domain and dimension
heatmap_data = df.groupby(['domain', 'dimension'])['mean_score'].mean().reset_index()
heatmap_pivot = heatmap_data.pivot(index='dimension', columns='domain', values='mean_score')

# Reorder dimensions by overall intensity
dimension_order = heatmap_pivot.mean(axis=1).sort_values(ascending=False).index
heatmap_pivot = heatmap_pivot.reindex(dimension_order)

# Create heatmap
plt.figure(figsize=(12, 8))
sns.heatmap(heatmap_pivot, annot=True, fmt='.3f', cmap='YlOrRd', 
            cbar_kws={'label': 'Mean Score'}, linewidths=0.5)
plt.title('Average BERT Dimension Scores by Topic Domain\n(Higher = Stronger Signal)', 
          fontsize=14, pad=20)
plt.xlabel('Topic Domain', fontsize=12)
plt.ylabel('BERT Dimension', fontsize=12)
plt.xticks(rotation=45, ha='right')
plt.yticks(rotation=0)
plt.tight_layout()
plt.savefig('visualizations/domain_dimension_heatmap.png', dpi=300, bbox_inches='tight')
print("  ✓ Saved: visualizations/domain_dimension_heatmap.png")

# 2. Top topics per dimension
print("\n2. Creating top topics visualization...")

fig, axes = plt.subplots(3, 3, figsize=(18, 14))
axes = axes.flatten()

for idx, dimension in enumerate(df['dimension'].unique()):
    dim_data = df[df['dimension'] == dimension].copy()
    
    # Get top 10 topics
    top_topics = dim_data.nlargest(10, 'mean_score')
    
    # Shorten topic names for readability
    top_topics['short_topic'] = top_topics['topic'].str.split(':').str[0]
    
    ax = axes[idx]
    bars = ax.barh(range(len(top_topics)), top_topics['mean_score'], 
                   color=sns.color_palette("viridis", len(top_topics)))
    ax.set_yticks(range(len(top_topics)))
    ax.set_yticklabels(top_topics['short_topic'], fontsize=8)
    ax.set_xlabel('Mean Score', fontsize=9)
    ax.set_title(dimension.replace('_', ' ').title(), fontsize=10, pad=10)
    ax.invert_yaxis()
    ax.grid(axis='x', alpha=0.3)

plt.suptitle('Top 10 Topics by BERT Dimension', fontsize=16, y=0.995)
plt.tight_layout()
plt.savefig('visualizations/top_topics_by_dimension.png', dpi=300, bbox_inches='tight')
print("  ✓ Saved: visualizations/top_topics_by_dimension.png")

# 3. Domain comparison across all dimensions
print("\n3. Creating domain comparison radar chart...")

# Calculate average scores per domain across all dimensions
domain_scores = df.groupby(['domain', 'dimension'])['mean_score'].mean().reset_index()

fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='polar'))

# Get unique dimensions
dimensions_list = domain_scores['dimension'].unique()
num_dims = len(dimensions_list)
angles = np.linspace(0, 2 * np.pi, num_dims, endpoint=False).tolist()
angles += angles[:1]  # Complete the circle

# Plot each domain
colors = sns.color_palette("Set2", n_colors=6)
for idx, domain in enumerate(domain_scores['domain'].unique()):
    domain_data = domain_scores[domain_scores['domain'] == domain]
    values = [domain_data[domain_data['dimension'] == dim]['mean_score'].values[0] 
              for dim in dimensions_list]
    values += values[:1]  # Complete the circle
    
    ax.plot(angles, values, 'o-', linewidth=2, label=domain, color=colors[idx])
    ax.fill(angles, values, alpha=0.15, color=colors[idx])

ax.set_xticks(angles[:-1])
ax.set_xticklabels([d.replace('_', '\n').title() for d in dimensions_list], 
                    fontsize=9)
ax.set_ylim(0, max(domain_scores['mean_score']) * 1.1)
ax.set_title('Domain Profiles Across All BERT Dimensions', 
             fontsize=14, pad=30)
ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.1), fontsize=10)
ax.grid(True)

plt.tight_layout()
plt.savefig('visualizations/domain_radar_chart.png', dpi=300, bbox_inches='tight')
print("  ✓ Saved: visualizations/domain_radar_chart.png")

# 4. Sample distribution for key dimension
print("\n4. Creating sample distribution plot...")

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
key_dimensions = ['emotional_valence_shift', 'certainty_collapse', 
                  'agency_reversal', 'temporal_bleed']

for idx, dimension in enumerate(key_dimensions):
    ax = axes[idx // 2, idx % 2]
    
    dim_data = df[df['dimension'] == dimension].copy()
    
    # Get top 5 and bottom 5
    top5 = dim_data.nlargest(5, 'mean_score')
    bottom5 = dim_data.nsmallest(5, 'mean_score')
    combined = pd.concat([top5, bottom5])
    
    combined['short_topic'] = combined['topic'].str.split(':').str[0]
    combined = combined.sort_values('mean_score')
    
    colors = ['red' if x > combined['mean_score'].median() else 'blue' 
              for x in combined['mean_score']]
    
    ax.barh(range(len(combined)), combined['mean_score'], color=colors, alpha=0.7)
    ax.set_yticks(range(len(combined)))
    ax.set_yticklabels(combined['short_topic'], fontsize=8)
    ax.set_xlabel('Score', fontsize=10)
    ax.set_title(dimension.replace('_', ' ').title(), fontsize=11)
    ax.axvline(combined['mean_score'].median(), color='black', 
               linestyle='--', alpha=0.5, linewidth=1)
    ax.grid(axis='x', alpha=0.3)

plt.suptitle('Crisis Dimensions: Highest vs Lowest Topics', fontsize=14)
plt.tight_layout()
plt.savefig('visualizations/crisis_dimensions_comparison.png', dpi=300, bbox_inches='tight')
print("  ✓ Saved: visualizations/crisis_dimensions_comparison.png")

print("\n✓ All visualizations created in visualizations/ directory")
print("\nGenerated files:")
print("  1. domain_dimension_heatmap.png - Overview of all correlations")
print("  2. top_topics_by_dimension.png - Top 10 topics for each dimension")
print("  3. domain_radar_chart.png - Domain profiles across dimensions")
print("  4. crisis_dimensions_comparison.png - High vs low crisis topics")