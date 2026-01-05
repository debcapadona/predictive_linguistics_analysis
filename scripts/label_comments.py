"""
Label all comments with discovered topics using trained BERTopic model
"""
import pandas as pd
import psycopg2
from bertopic import BERTopic
from datetime import datetime

print("Loading BERTopic model...")
topic_model = BERTopic.load("models/bertopic_model")

print("Connecting to database...")
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='linguistic_predictor_v2',
    user='analyzer',
    password='dev_password_change_in_prod'
)

print("Reconstructing text from word_tokens...")
# Process in batches for memory efficiency
batch_size = 5000
cur = conn.cursor()

# Get total count
cur.execute("SELECT COUNT(DISTINCT story_id) FROM word_tokens")
total_stories = cur.fetchone()[0]
print(f"Total stories to process: {total_stories}")

processed = 0
labeled = 0

while processed < total_stories:
    print(f"\nProcessing batch {processed//batch_size + 1} ({processed}/{total_stories})...")
    
    # Fetch batch of stories with reconstructed text
    query = f"""
        WITH story_texts AS (
            SELECT 
                wt.story_id,
                STRING_AGG(wt.word_text, ' ' ORDER BY wt.position) as text
            FROM word_tokens wt
            GROUP BY wt.story_id
            HAVING COUNT(*) > 5
        )
        SELECT story_id, text
        FROM story_texts
        ORDER BY story_id
        OFFSET {processed}
        LIMIT {batch_size}
    """
    
    df = pd.read_sql(query, conn)
    
    if len(df) == 0:
        break
    
    print(f"  Loaded {len(df)} stories")
    
    # Predict topics
    print(f"  Predicting topics...")
    documents = df['text'].tolist()
    topics, probs = topic_model.transform(documents)
    
    # Insert labels into database
    print(f"  Inserting labels...")
    for idx, (story_id, topic_id) in enumerate(zip(df['story_id'], topics)):
        if topic_id == -1:  # Skip outliers
            continue
        
        # Get the max probability (confidence for the assigned topic)
        prob = float(probs[idx].max())
        
        # Get the max probability (confidence for the assigned topic)
        prob = float(probs[idx].max())
        
        # Map BERTopic topic_id to our taxonomy tier3 id
        cur.execute("""
            SELECT id FROM topic_taxonomy 
            WHERE topic_name LIKE %s AND tier = 3
        """, (f"Topic_{topic_id}:%",))
        
        result = cur.fetchone()
        if result:
            taxonomy_id = result[0]
            
            # Skip if story_id is empty or invalid
            if not story_id or story_id.strip() == '':
                continue
            
            # Insert comment label
            cur.execute("""
                INSERT INTO comment_labels 
                (comment_id, label_type, topic_id, confidence, source, labeled_at, labeled_by)
                VALUES (%s, 'topic', %s, %s, 'bertopic', NOW(), 'bertopic_model')
                ON CONFLICT DO NOTHING
            """, (story_id, taxonomy_id, float(prob)))
            
            labeled += 1
    
    conn.commit()
    processed += len(df)
    print(f"  ✓ Batch complete. Total labeled: {labeled}")

print(f"\n✓ Labeling complete!")
print(f"  Total stories processed: {processed}")
print(f"  Total labels created: {labeled}")

# Show statistics
print("\nLabel distribution by Tier 1 domain:")
cur.execute("""
    SELECT 
        t1.topic_name as domain,
        COUNT(cl.id) as label_count,
        ROUND(AVG(cl.confidence)::numeric, 3) as avg_confidence
    FROM comment_labels cl
    JOIN topic_taxonomy t3 ON cl.topic_id = t3.id
    JOIN topic_taxonomy t2 ON t3.parent_id = t2.id
    JOIN topic_taxonomy t1 ON t2.parent_id = t1.id
    WHERE t1.tier = 1
    GROUP BY t1.topic_name
    ORDER BY label_count DESC
""")

for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]:,} labels (avg confidence: {row[2]})")

cur.close()
conn.close()

print("\n✓ Done! Comments are labeled and ready for word propagation.")