"""
Propagate topic labels from comments to words
Each word inherits all topic labels from its parent comment
"""
import psycopg2
from datetime import datetime

print("Connecting to database...")
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='linguistic_predictor_v2',
    user='analyzer',
    password='dev_password_change_in_prod'
)
cur = conn.cursor()

print("\nPropagating labels from comments to words...")
print("This will take a few minutes...")

# Insert word labels by joining word_tokens with comment_labels
cur.execute("""
    INSERT INTO word_labels 
    (word, label_type, label_value, confidence, source, labeled_at, labeled_by, notes)
    SELECT DISTINCT
        wt.word_lower,
        'topic',
        t3.topic_name,
        cl.confidence,
       'comment_propagated',
        NOW(),
        cl.labeled_by,
        'Inherited from comment: ' || cl.comment_id
    FROM word_tokens wt
    JOIN comment_labels cl ON wt.story_id = cl.comment_id::text
    JOIN topic_taxonomy t3 ON cl.topic_id = t3.id
    WHERE cl.label_type = 'topic'
    ON CONFLICT (word, label_type, label_value) DO NOTHING
""")

rows_inserted = cur.rowcount
conn.commit()

print(f"✓ Propagated labels to {rows_inserted:,} word-label pairs")

# Get statistics
print("\nGathering statistics...")

# Total unique words labeled
cur.execute("""
    SELECT COUNT(DISTINCT word) 
    FROM word_labels 
    WHERE label_type = 'topic'
""")
unique_words = cur.fetchone()[0]

# Average labels per word
cur.execute("""
    SELECT AVG(label_count) 
    FROM (
        SELECT word, COUNT(*) as label_count
        FROM word_labels 
        WHERE label_type = 'topic'
        GROUP BY word
    ) subq
""")
avg_labels = cur.fetchone()[0]

# Top labeled words by topic diversity
print(f"\nTop 10 words by topic diversity:")
cur.execute("""
    SELECT 
        word,
        COUNT(DISTINCT label_value) as topic_count,
        ROUND(AVG(confidence)::numeric, 3) as avg_confidence
    FROM word_labels
    WHERE label_type = 'topic'
    GROUP BY word
    ORDER BY topic_count DESC, word
    LIMIT 10
""")

for row in cur.fetchall():
    print(f"  '{row[0]}': {row[1]} topics (avg confidence: {row[2]})")

# Distribution by tier 1 domain
print(f"\nWord labels by Tier 1 domain:")
cur.execute("""
    SELECT 
        t1.topic_name as domain,
        COUNT(wl.id) as word_label_count
    FROM word_labels wl
    JOIN topic_taxonomy t3 ON wl.label_value = t3.topic_name
    JOIN topic_taxonomy t2 ON t3.parent_id = t2.id
    JOIN topic_taxonomy t1 ON t2.parent_id = t1.id
    WHERE t1.tier = 1 AND wl.label_type = 'topic'
    GROUP BY t1.topic_name
    ORDER BY word_label_count DESC
""")

for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]:,} word-label pairs")

print(f"\n✓ Summary:")
print(f"  Unique words labeled: {unique_words:,}")
print(f"  Average topics per word: {avg_labels:.2f}")
print(f"  Total word-label pairs: {rows_inserted:,}")

cur.close()
conn.close()

print("\n✓ Done! Words now have topic labels inherited from their comments.")