"""
Score Reddit comments using existing BERT models (.pt files)
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

# Check data availability
conn = sqlite3.connect('data/reddit_data.db')
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM reddit_comments WHERE body NOT IN ('[deleted]', '[removed]')")
valid_comments = cur.fetchone()[0]

print(f"\nValid comments available: {valid_comments:,}")

# Focus on June-July for now (3 test cases)
print("\nFiltering to June-July 2024 for initial analysis...")
query = """
    SELECT id, body, created_utc, subreddit
    FROM reddit_comments
    WHERE body NOT IN ('[deleted]', '[removed]')
    AND LENGTH(body) > 10
    AND created_utc BETWEEN 1717804800 AND 1722470400
    ORDER BY created_utc
"""

df = pd.read_sql(query, conn)
print(f"  ✓ Loaded {len(df):,} June-July comments")

# 9 BERT dimensions
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

# Model paths
model_dir = 'models'

# Create tables in Reddit database
print("\nCreating BERT dimension tables...")

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

# Load base model and tokenizer (same for all dimensions)
print("\nLoading base BERT model...")
base_model_name = "bert-base-uncased"
tokenizer = AutoTokenizer.from_pretrained(base_model_name)
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"  ✓ Using device: {device}")

# Score with each dimension
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
    
    # Load model weights
    model_path = f'{model_dir}/bert_{dimension}.pt'
    
    if not os.path.exists(model_path):
        print(f"  ✗ Model not found: {model_path}")
        continue
    
    print(f"  Loading model weights from {model_path}...")
    
    # Create model architecture
    model = AutoModelForSequenceClassification.from_pretrained(
        base_model_name,
        num_labels=2
    )
    
    # Load trained weights
    model.load_state_dict(torch.load(model_path, map_location=device))
    model.to(device)
    model.eval()
    
    # Score in batches
    batch_size = 16  # Smaller batches for safety
    scores = []
    
    print(f"  Scoring {len(df):,} comments...")
    
    for i in tqdm(range(0, len(df), batch_size)):
        batch = df.iloc[i:i+batch_size]
        texts = batch['body'].tolist()
        
        with torch.no_grad():
            inputs = tokenizer(texts, padding=True, truncation=True, 
                             max_length=512, return_tensors='pt')
            inputs = {k: v.to(device) for k, v in inputs.items()}
            
            try:
                outputs = model(**inputs)
                batch_scores = torch.softmax(outputs.logits, dim=1)[:, 1].cpu().numpy()
            except Exception as e:
                print(f"    Error on batch {i}: {e}")
                batch_scores = [0.0] * len(texts)
        
        for comment_id, score in zip(batch['id'], batch_scores):
            scores.append((comment_id, float(score)))
    
    # Save to database
    print(f"  Saving {len(scores):,} scores...")
    cur.executemany(f"""
        INSERT INTO reddit_bert_{dimension} (comment_id, score)
        VALUES (?, ?)
    """, scores)
    conn.commit()
    
    print(f"  ✓ {dimension} complete ({len(scores):,} scores)")
    
    # Clear memory
    del model
    torch.cuda.empty_cache()

conn.close()

print("\n" + "="*70)
print("BERT SCORING COMPLETE")
print("="*70)
print("\nScored June-July Reddit comments with 9 BERT dimensions")
print("Ready for cross-platform validation!")