"""
Score Reddit comments using existing BERT models
Then propagate labels to words
"""
import pandas as pd
import sqlite3
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from tqdm import tqdm
import os

print("="*70)
print("SCORING REDDIT COMMENTS WITH BERT")
print("="*70)

# Check if scraping is complete
conn = sqlite3.connect('data/reddit_data.db')
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM reddit_posts WHERE comments_scraped = 1")
scraped = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM reddit_posts")
total = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM reddit_comments WHERE body NOT IN ('[deleted]', '[removed]')")
valid_comments = cur.fetchone()[0]

print(f"\nPosts scraped: {scraped:,}/{total:,}")
print(f"Valid comments: {valid_comments:,}")

if scraped < total:
    print(f"\n⚠️  Scraping still in progress ({scraped/total*100:.1f}% done)")
    response = input("Continue anyway? (y/n): ")
    if response.lower() != 'y':
        exit()

# Load comments
print(f"\nLoading {valid_comments:,} Reddit comments...")
query = """
    SELECT id, body, created_utc, subreddit
    FROM reddit_comments
    WHERE body NOT IN ('[deleted]', '[removed]')
    AND LENGTH(body) > 10
    ORDER BY created_utc
"""

df = pd.read_sql(query, conn)
print(f"  ✓ Loaded {len(df):,} comments")

# 9 BERT dimensions (same as HN)
dimensions = [
    'emotional_valence_shift',
    'temporal_bleed',
    'certainty_collapse',
    'time_compression',
    'agency_reversal',
    'metaphor_cluster_density',
    'novel_meme_explosion',
    'sacred_profane_ratio',
    'pronoun_flip'
]

# Check if BERT models exist
model_dir = 'bert_models'
if not os.path.exists(model_dir):
    print(f"\n✗ BERT models not found in {model_dir}/")
    print("You need to run the BERT training pipeline first")
    exit()

# Create tables in Reddit database for BERT scores
print("\nCreating BERT dimension tables in Reddit database...")

for dimension in dimensions:
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS reddit_bert_{dimension} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            comment_id TEXT NOT NULL,
            score REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (comment_id) REFERENCES reddit_comments(id)
        )
    """)
    
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_reddit_bert_{dimension}_comment ON reddit_bert_{dimension}(comment_id)")

conn.commit()
print("  ✓ Tables created")

# Score comments with each dimension
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"\nUsing device: {device}")

for dimension in dimensions:
    print(f"\n{'='*70}")
    print(f"Processing: {dimension}")
    print(f"{'='*70}")
    
    # Check if already processed
    cur.execute(f"SELECT COUNT(*) FROM reddit_bert_{dimension}")
    existing = cur.fetchone()[0]
    
    if existing > 0:
        print(f"  ⚠️  {existing:,} scores already exist")
        response = input(f"  Reprocess {dimension}? (y/n): ")
        if response.lower() != 'y':
            continue
        cur.execute(f"DELETE FROM reddit_bert_{dimension}")
        conn.commit()
    
    # Load model
    model_path = f'{model_dir}/{dimension}'
    
    if not os.path.exists(model_path):
        print(f"  ✗ Model not found: {model_path}")
        print(f"  Skipping {dimension}")
        continue
    
    print(f"  Loading model from {model_path}...")
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    model = AutoModelForSequenceClassification.from_pretrained(model_path)
    model.to(device)
    model.eval()
    
    # Score in batches
    batch_size = 32
    scores = []
    
    print(f"  Scoring {len(df):,} comments...")
    
    for i in tqdm(range(0, len(df), batch_size)):
        batch = df.iloc[i:i+batch_size]
        texts = batch['body'].tolist()
        
        with torch.no_grad():
            inputs = tokenizer(texts, padding=True, truncation=True, 
                             max_length=512, return_tensors='pt')
            inputs = {k: v.to(device) for k, v in inputs.items()}
            outputs = model(**inputs)
            batch_scores = torch.softmax(outputs.logits, dim=1)[:, 1].cpu().numpy()
        
        for comment_id, score in zip(batch['id'], batch_scores):
            scores.append((comment_id, float(score)))
    
    # Save to database
    print(f"  Saving {len(scores):,} scores...")
    cur.executemany(f"""
        INSERT INTO reddit_bert_{dimension} (comment_id, score)
        VALUES (?, ?)
    """, scores)
    conn.commit()
    
    print(f"  ✓ {dimension} complete")
    
    # Clear memory
    del model, tokenizer
    torch.cuda.empty_cache()

conn.close()

print("\n" + "="*70)
print("BERT SCORING COMPLETE")
print("="*70)
print("\nNext steps:")
print("  1. Analyze dimension scores by date")
print("  2. Compare Reddit vs HN on test case dates")
print("  3. Run: python scripts/compare_reddit_hn_signals.py")