"""
Fix Reddit topic labels - map BERTopic topics to taxonomy properly
Matches HN labeling process exactly
"""
import pandas as pd
import sqlite3
import psycopg2
from bertopic import BERTopic
from tqdm import tqdm

print("="*70)
print("FIXING REDDIT TOPIC LABELS (PROPER TAXONOMY MAPPING)")
print("="*70)

# Load BERTopic model
model_path = 'models/bertopic_model'
print(f"\nLoading BERTopic model...")
topic_model = BERTopic.load(model_path)

# Connect to databases
reddit_conn = sqlite3.connect('data/reddit_snapshot_dec29.db')
reddit_cur = reddit_conn.cursor()

pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)
pg_cur = pg_conn.cursor()

# Get taxonomy mapping (BERTopic topic_id -> taxonomy ID)
print("\nLoading taxonomy mapping...")
pg_cur.execute("""
    SELECT id, topic_name 
    FROM topic_taxonomy 
    WHERE tier = 3
""")
taxonomy_map = {}
for tax_id, topic_name in pg_cur.fetchall():
    # Extract BERTopic number from name like "Topic_9: 9_rust_ada..."
    if topic_name.startswith('Topic_'):
        bertopic_num = int(topic_name.split(':')[0].replace('Topic_', ''))
        taxonomy_map[bertopic_num] = tax_id

print(f"  ✓ Loaded {len(taxonomy_map)} topic mappings")

# Clear existing labels
print("\nClearing existing topic labels...")
reddit_cur.execute("DELETE FROM reddit_comment_labels WHERE label_type = 'topic'")
reddit_conn.commit()

# Load Reddit comments
print("Loading Reddit comments...")
query = """
    SELECT id, body
    FROM reddit_comments
    WHERE body NOT IN ('[deleted]', '[removed]')
    AND LENGTH(body) > 50
    AND DATE(datetime(created_utc, 'unixepoch')) BETWEEN '2024-01-01' AND '2024-11-30'
"""
df = pd.read_sql(query, reddit_conn)
print(f"  ✓ Loaded {len(df):,} comments")

# Predict topics in batches
print("\nLabeling comments with taxonomy-mapped topics...")
batch_size = 5000
total_labels = 0

for i in tqdm(range(0, len(df), batch_size)):
    batch = df.iloc[i:i+batch_size]
    
    # Predict topics
    topics, probs = topic_model.transform(batch['body'].tolist())
    
    # Save labels with taxonomy mapping
    labels = []
    for comment_id, bertopic_id, prob in zip(batch['id'], topics, probs):
        if bertopic_id == -1:  # Skip outliers
            continue
        
        # Map BERTopic ID to taxonomy ID
        taxonomy_id = taxonomy_map.get(bertopic_id)
        if taxonomy_id is None:
            continue  # Skip if no mapping found
        
        confidence = float(prob.max()) if hasattr(prob, 'max') else float(prob)
        
        labels.append((
            comment_id,
            'topic',
            taxonomy_id,  # Use taxonomy ID, not BERTopic ID
            confidence,
            'bertopic'
        ))
    
    # Insert batch
    if labels:
        reddit_cur.executemany("""
            INSERT INTO reddit_comment_labels 
            (comment_id, label_type, topic_id, confidence, source)
            VALUES (?, ?, ?, ?, ?)
        """, labels)
        reddit_conn.commit()
        total_labels += len(labels)

print(f"\n✓ Created {total_labels:,} topic labels")

# Verify
reddit_cur.execute("""
    SELECT COUNT(*), MIN(topic_id), MAX(topic_id) 
    FROM reddit_comment_labels 
    WHERE label_type = 'topic'
""")
count, min_topic, max_topic = reddit_cur.fetchone()
print(f"  Labeled: {count:,} comments")
print(f"  Topic ID range: {min_topic} to {max_topic}")

# Show distribution
print("\nTop 10 topics on Reddit:")
reddit_cur.execute("""
    SELECT topic_id, COUNT(*) as count
    FROM reddit_comment_labels
    WHERE label_type = 'topic'
    GROUP BY topic_id
    ORDER BY count DESC
    LIMIT 10
""")

for topic_id, count in reddit_cur.fetchall():
    pg_cur.execute("SELECT topic_name FROM topic_taxonomy WHERE id = %s", (topic_id,))
    topic_name = pg_cur.fetchone()[0]
    print(f"  {topic_name[:50]:50s} | {count:6,} comments")

reddit_conn.close()
pg_conn.close()

print("\n" + "="*70)
print("DONE - Reddit now uses same taxonomy as HN!")
print("="*70)