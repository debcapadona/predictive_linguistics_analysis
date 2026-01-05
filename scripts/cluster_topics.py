"""
Use HDBSCAN to automatically discover clusters of topics
based on their BERT dimension profiles
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
import hdbscan
import umap

print("Loading topic-dimension data...")
df = pd.read_csv('data/topic_dimension_correlations.csv')

# Pivot to get topic × dimension matrix
print("Creating feature matrix...")
topic_features = df.pivot_table(
    index='topic',
    columns='dimension',
    values='mean_score',
    aggfunc='mean'
).fillna(0)

print(f"Feature matrix: {topic_features.shape[0]} topics × {topic_features.shape[1]} dimensions")

# Standardize features (important for clustering)
scaler = StandardScaler()
features_scaled = scaler.fit_transform(topic_features.values)

# Reduce dimensionality with UMAP for visualization
print("\nReducing to 2D with UMAP...")
reducer = umap.UMAP(
    n_neighbors=15,
    min_dist=0.1,
    n_components=2,
    metric='euclidean',
    random_state=42
)
embedding = reducer.fit_transform(features_scaled)

# Cluster with HDBSCAN
print("Clustering with HDBSCAN...")
clusterer = hdbscan.HDBSCAN(
    min_cluster_size=3,
    min_samples=2,
    metric='euclidean',
    cluster_selection_epsilon=0.5
)
clusters = clusterer.fit_predict(features_scaled)

# Add results to dataframe
topic_features['cluster'] = clusters
topic_features['umap_x'] = embedding[:, 0]
topic_features['umap_y'] = embedding[:, 1]
topic_features['topic_name'] = topic_features.index

# Get domain info
domain_map = df[['topic', 'domain']].drop_duplicates().set_index('topic')['domain']
topic_features['domain'] = topic_features.index.map(domain_map)

print(f"\nFound {clusters.max() + 1} clusters (plus {(clusters == -1).sum()} outliers)")

# Show cluster statistics
print("\nCluster breakdown:")
for cluster_id in sorted(topic_features['cluster'].unique()):
    cluster_topics = topic_features[topic_features['cluster'] == cluster_id]
    if cluster_id == -1:
        print(f"\nOutliers (n={len(cluster_topics)}):")
    else:
        print(f"\nCluster {cluster_id} (n={len(cluster_topics)}):")
    
    # Show domains in cluster
    domain_counts = cluster_topics['domain'].value_counts()
    print(f"  Domains: {dict(domain_counts)}")
    
    # Show average dimension scores
    dim_means = cluster_topics[df['dimension'].unique()].mean()
    top_dims = dim_means.nlargest(3)
    print(f"  Top dimensions:")
    for dim, score in top_dims.items():
        print(f"    - {dim}: {score:.3f}")
    
    # Show sample topics
    print(f"  Sample topics:")
    for topic in cluster_topics.index[:3]:
        short_name = topic.split(':')[0] if ':' in topic else topic[:50]
        print(f"    - {short_name}")

# Visualize clusters
print("\nCreating cluster visualization...")

fig, axes = plt.subplots(1, 2, figsize=(16, 7))

# Plot 1: Clusters in UMAP space
ax = axes[0]
scatter = ax.scatter(
    topic_features['umap_x'],
    topic_features['umap_y'],
    c=topic_features['cluster'],
    cmap='tab10',
    s=100,
    alpha=0.6,
    edgecolors='black',
    linewidth=0.5
)

# Label each point with topic number
for idx, row in topic_features.iterrows():
    topic_num = idx.split('_')[1].split(':')[0] if '_' in idx else ''
    ax.annotate(topic_num, (row['umap_x'], row['umap_y']), 
                fontsize=7, ha='center', va='center')

ax.set_xlabel('UMAP Dimension 1', fontsize=12)
ax.set_ylabel('UMAP Dimension 2', fontsize=12)
ax.set_title('Topic Clusters Based on BERT Dimension Profiles', fontsize=14)
ax.grid(True, alpha=0.3)

# Add cluster legend
unique_clusters = sorted([c for c in topic_features['cluster'].unique() if c != -1])
legend_elements = [plt.Line2D([0], [0], marker='o', color='w', 
                              markerfacecolor=plt.cm.tab10(i/10), 
                              markersize=10, label=f'Cluster {c}')
                   for i, c in enumerate(unique_clusters)]
legend_elements.append(plt.Line2D([0], [0], marker='o', color='w',
                                 markerfacecolor='gray',
                                 markersize=10, label='Outliers'))
ax.legend(handles=legend_elements, loc='best', fontsize=9)

# Plot 2: Domain distribution per cluster
ax = axes[1]
cluster_domain_counts = pd.crosstab(topic_features['cluster'], topic_features['domain'])
cluster_domain_counts.plot(kind='bar', stacked=True, ax=ax, 
                           colormap='Set3', width=0.7)
ax.set_xlabel('Cluster ID', fontsize=12)
ax.set_ylabel('Number of Topics', fontsize=12)
ax.set_title('Domain Composition of Each Cluster', fontsize=14)
ax.legend(title='Domain', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
ax.grid(axis='y', alpha=0.3)
plt.setp(ax.xaxis.get_majorticklabels(), rotation=0)

plt.tight_layout()
plt.savefig('visualizations/topic_clusters_hdbscan.png', dpi=300, bbox_inches='tight')
print("  ✓ Saved: visualizations/topic_clusters_hdbscan.png")

# Save cluster assignments
cluster_results = topic_features[['cluster', 'domain', 'umap_x', 'umap_y']].copy()
cluster_results.to_csv('data/topic_clusters.csv')
print("\n✓ Saved cluster assignments to data/topic_clusters.csv")

# Create detailed cluster profiles
print("\nCreating cluster dimension profiles heatmap...")
fig, ax = plt.subplots(figsize=(12, 8))

# Get mean dimension scores per cluster
cluster_profiles = topic_features.groupby('cluster')[df['dimension'].unique()].mean()
cluster_profiles = cluster_profiles.loc[cluster_profiles.index != -1]  # Exclude outliers

sns.heatmap(cluster_profiles.T, annot=True, fmt='.3f', cmap='RdYlGn', 
            center=cluster_profiles.values.mean(),
            cbar_kws={'label': 'Mean Dimension Score'},
            linewidths=0.5, ax=ax)
ax.set_xlabel('Cluster ID', fontsize=12)
ax.set_ylabel('BERT Dimension', fontsize=12)
ax.set_title('Cluster Profiles: Average Dimension Scores', fontsize=14, pad=15)
plt.tight_layout()
plt.savefig('visualizations/cluster_dimension_profiles.png', dpi=300, bbox_inches='tight')
print("  ✓ Saved: visualizations/cluster_dimension_profiles.png")

print("\n✓ Done! Clusters identified and visualized.")
print(f"\nDiscovered {clusters.max() + 1} distinct topic clusters")
print("Check visualizations/topic_clusters_hdbscan.png to see the groupings")