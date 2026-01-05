"""
Discover topic taxonomy from HN comments using BERTopic
"""
from bertopic import BERTopic
import pandas as pd
import psycopg2

# Connect to Postgres
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='linguistic_predictor_v2',
    user='analyzer',
    password='dev_password_change_in_prod'
)

print("Reconstructing text from word_tokens...")
# Reconstruct stories by joining words in position order
query = """
    WITH story_texts AS (
        SELECT 
            wt.story_id,
            STRING_AGG(wt.word_text, ' ' ORDER BY wt.position) as text
        FROM word_tokens wt
        GROUP BY wt.story_id
        HAVING COUNT(*) > 10  -- Filter very short texts
    ),
    sampled_stories AS (
        SELECT story_id, text
        FROM story_texts
        ORDER BY RANDOM()
        LIMIT 10000
    )
    SELECT story_id, text
    FROM sampled_stories
    WHERE LENGTH(text) > 50
"""

df = pd.read_sql(query, conn)
print(f"Loaded {len(df)} stories")

# Prepare documents
documents = df['text'].tolist()
print(f"Processing {len(documents)} documents")

print("\nRunning BERTopic (this will take 5-10 minutes)...")
topic_model = BERTopic(
    language="english",
    calculate_probabilities=True,
    verbose=True,
    nr_topics="auto"
)

# Fit the model
topics, probs = topic_model.fit_transform(documents)

# Get topic info
topic_info = topic_model.get_topic_info()
print(f"\nDiscovered {len(topic_info) - 1} topics (excluding outliers)")
print("\nTop 30 topics:")
print(topic_info.head(30))

# Get hierarchical structure
print("\nGenerating hierarchical structure...")
hierarchical_topics = topic_model.hierarchical_topics(documents)

# Save results
print("\nSaving results...")
topic_info.to_csv('data/discovered_topics.csv', index=False)
hierarchical_topics.to_csv('data/topic_hierarchy.csv', index=False)

# Save the model
topic_model.save("models/bertopic_model")

print("\n✓ Done! Check:")
print("  - data/discovered_topics.csv")
print("  - data/topic_hierarchy.csv")

conn.close()