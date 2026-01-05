"""
Analyze correlations between topics and BERT dimensions
Find which topics predict high/low scores on each dimension
"""
import pandas as pd
import psycopg2
import numpy as np
from scipy.stats import pearsonr

print("Connecting to database...")
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='linguistic_predictor_v2',
    user='analyzer',
    password='dev_password_change_in_prod'
)

# List of dimension tables
dimensions = [
    'emotional_valence_shift',
    'temporal_bleed',
    'certainty_collapse',
    'time_compression',
    'sacred_profane_ratio',
    'pronoun_flip',
    'agency_reversal',
    'metaphor_cluster_density',
    'novel_meme_explosion'
]

results = []

for dimension in dimensions:
    print(f"\nAnalyzing {dimension}...")
    
    # Get stories with both dimension scores and topic labels
    query = f"""
        SELECT 
            t1.topic_name as domain,
            t2.topic_name as category,
            t3.topic_name as topic,
            bd.score,
            cl.confidence
        FROM bert_{dimension} bd
        JOIN comment_labels cl ON bd.story_id = cl.comment_id::text
        JOIN topic_taxonomy t3 ON cl.topic_id = t3.id
        JOIN topic_taxonomy t2 ON t3.parent_id = t2.id
        JOIN topic_taxonomy t1 ON t2.parent_id = t1.id
        WHERE cl.label_type = 'topic'
        AND t3.tier = 3
    """
    
    df = pd.read_sql(query, conn)
    print(f"  Loaded {len(df)} story-topic pairs")
    
    if len(df) == 0:
        continue
    
    # Calculate average score per topic
    topic_scores = df.groupby(['domain', 'category', 'topic']).agg({
        'score': ['mean', 'std', 'count']
    }).reset_index()
    
    topic_scores.columns = ['domain', 'category', 'topic', 'mean_score', 'std_score', 'count']
    topic_scores = topic_scores[topic_scores['count'] >= 10]  # Require at least 10 samples
    
    # Store results
    for _, row in topic_scores.iterrows():
        results.append({
            'dimension': dimension,
            'domain': row['domain'],
            'category': row['category'],
            'topic': row['topic'],
            'mean_score': row['mean_score'],
            'std_score': row['std_score'],
            'sample_count': row['count']
        })
    
    # Show top correlations for this dimension
    print(f"\n  Top 5 topics with HIGHEST {dimension}:")
    top = topic_scores.nlargest(5, 'mean_score')
    for _, row in top.iterrows():
        print(f"    {row['topic'][:50]}: {row['mean_score']:.3f} (n={row['count']})")
    
    print(f"\n  Top 5 topics with LOWEST {dimension}:")
    bottom = topic_scores.nsmallest(5, 'mean_score')
    for _, row in bottom.iterrows():
        print(f"    {row['topic'][:50]}: {row['mean_score']:.3f} (n={row['count']})")

# Save full results
results_df = pd.DataFrame(results)
results_df.to_csv('data/topic_dimension_correlations.csv', index=False)
print(f"\n✓ Saved full results to data/topic_dimension_correlations.csv")

# Show summary statistics
print("\n" + "="*70)
print("SUMMARY: Domain-level correlations")
print("="*70)

for dimension in dimensions:
    dim_data = results_df[results_df['dimension'] == dimension]
    
    if len(dim_data) == 0:
        continue
    
    domain_avg = dim_data.groupby('domain')['mean_score'].mean().sort_values(ascending=False)
    
    print(f"\n{dimension}:")
    for domain, score in domain_avg.items():
        print(f"  {domain}: {score:.3f}")

conn.close()

print("\n✓ Done! Check data/topic_dimension_correlations.csv for full results.")