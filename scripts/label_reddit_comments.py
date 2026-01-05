"""
Label Reddit comments with BERTopic topics
Uses the same trained model from HN analysis
"""
import pandas as pd
import sqlite3
from bertopic import BERTopic
from tqdm import tqdm
import re

print("="*70)
print("LABELING REDDIT COMMENTS WITH TOPICS")
print("="*70)

# Load BERTopic model trained on HN
model_path = 'models/bertopic_model'
print(f"\nLoading BERTopic model from {model_path}...")

try:
    topic_model = BERTopic.load(model_path)
    print("  ✓ Model loaded")
except Exception as e:
    print(f"  ✗ Error: {e}")
    print("\nYou need the trained BERTopic model from HN analysis")
    exit()

# Connect to databases
print("\nConnecting to databases...")
reddit_conn = sqlite3.connect('data/reddit_snapshot_dec29.db')
reddit_cur = reddit_conn.cursor()

# Get topic taxonomy from PostgreSQL
print("Loading topic taxonomy from PostgreSQL...")
import psycopg2
pg_conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='linguistic_predictor_v2',
    user='analyzer',
    password='dev_password_change_in_prod'
)
pg_cur = pg_conn.cursor()

pg_cur.execute("SELECT id, topic_name FROM topic_taxonomy WHERE tier = 3")
topic_map = {row[0]: row[1] for row in pg_cur.fetchall()}
print(f"  ✓ Loaded {len(topic_map)} topics")

# Create labels table in Reddit database
print("\nCreating Reddit comment_labels table...")
reddit_cur.execute("""
    CREATE TABLE IF NOT EXISTS reddit_comment_labels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        comment_id TEXT NOT NULL,
        label_type VARCHAR(50) NOT NULL,
        topic_id INTEGER,
        confidence FLOAT,
        source VARCHAR(20) NOT NULL,
        labeled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (comment_id) REFERENCES reddit_comments(id)
    )
""")

reddit_cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_reddit_comment_labels_comment 
    ON reddit_comment_labels(comment_id)
""")

reddit_cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_reddit_comment_labels_topic 
    ON reddit_comment_labels(topic_id)
""")

reddit_conn.commit()
print("  ✓ Table created")

# Load Reddit comments
print("\nLoading Reddit comments...")
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
print("\nPredicting topics...")
batch_size = 5000
total_labels = 0

for i in tqdm(range(0, len(df), batch_size)):
    batch = df.iloc[i:i+batch_size]
    
    # Predict topics
    topics, probs = topic_model.transform(batch['body'].tolist())
    
    # Save labels
    labels = []
    for comment_id, topic_id, prob in zip(batch['id'], topics, probs):
        if topic_id != -1:  # Skip outliers
            # BERTopic topics are 0, 1, 2, etc.
            # We need to map to our taxonomy
            # For now, just use topic_id directly (we'll refine mapping later)
            confidence = float(prob.max()) if hasattr(prob, 'max') else float(prob)
            labels.append((
                comment_id,
                'topic',
                topic_id,  # Using BERTopic's topic number
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

# Create word labels table
print("\nCreating Reddit word_labels table...")
reddit_cur.execute("""
    CREATE TABLE IF NOT EXISTS reddit_word_labels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        word VARCHAR(100) NOT NULL,
        label_type VARCHAR(50) NOT NULL,
        topic_id INTEGER,
        confidence FLOAT,
        source VARCHAR(20) NOT NULL,
        labeled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

reddit_cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_reddit_word_labels_word 
    ON reddit_word_labels(word)
""")

reddit_conn.commit()
print("  ✓ Table created")

# Create word tokens table
print("\nCreating word tokens from comments...")

reddit_cur.execute("""
    CREATE TABLE IF NOT EXISTS reddit_word_tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        comment_id TEXT NOT NULL,
        word VARCHAR(100) NOT NULL,
        position INTEGER NOT NULL,
        FOREIGN KEY (comment_id) REFERENCES reddit_comments(id)
    )
""")

reddit_cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_reddit_word_tokens_comment 
    ON reddit_word_tokens(comment_id)
""")

# Tokenize comments
print("Tokenizing comments...")

token_batch = []
for i in tqdm(range(0, len(df), 1000)):
    batch = df.iloc[i:i+1000]
    
    for comment_id, body in zip(batch['id'], batch['body']):
        words = re.findall(r'\b\w+\b', body.lower())
        for pos, word in enumerate(words):
            if len(word) > 2:  # Skip very short words
                token_batch.append((comment_id, word, pos))
    
    # Insert batch
    if len(token_batch) >= 10000:
        reddit_cur.executemany("""
            INSERT INTO reddit_word_tokens (comment_id, word, position)
            VALUES (?, ?, ?)
        """, token_batch)
        reddit_conn.commit()
        token_batch = []

# Final insert
if token_batch:
    reddit_cur.executemany("""
        INSERT INTO reddit_word_tokens (comment_id, word, position)
        VALUES (?, ?, ?)
    """, token_batch)
    reddit_conn.commit()

print("  ✓ Tokenization complete")

# Propagate labels from comments to words
print("\nPropagating topic labels to words...")

query = """
    INSERT INTO reddit_word_labels (word, label_type, topic_id, confidence, source)
    SELECT DISTINCT
        wt.word,
        cl.label_type,
        cl.topic_id,
        AVG(cl.confidence) as avg_confidence,
        'propagated'
    FROM reddit_word_tokens wt
    JOIN reddit_comment_labels cl ON wt.comment_id = cl.comment_id
    WHERE cl.label_type = 'topic'
    GROUP BY wt.word, cl.label_type, cl.topic_id
"""

reddit_cur.execute(query)
reddit_conn.commit()

reddit_cur.execute("SELECT COUNT(*) FROM reddit_word_labels")
word_labels = reddit_cur.fetchone()[0]

print(f"  ✓ Created {word_labels:,} word-topic associations")

reddit_conn.close()
pg_conn.close()

print("\n" + "="*70)
print("LABELING COMPLETE")
print("="*70)
print(f"\nComment labels: {total_labels:,}")
print(f"Word labels: {word_labels:,}")
print("\nReddit comments are now labeled with same topics as HN!")