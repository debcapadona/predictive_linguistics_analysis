"""
Score Reddit comments incrementally - only process unscored comments
Can be run repeatedly as new data arrives
"""
import pandas as pd
import sqlite3
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from tqdm import tqdm

print("="*70)
print("INCREMENTAL REDDIT BERT SCORING")
print("="*70)

conn = sqlite3.connect('data/reddit_snapshot_dec29.db')
cur = conn.cursor()

# Process Jan-Nov 2024 (complete months)
print("\nLoading comments from Jan-Nov 2024...")
query = """
    SELECT id, body, created_utc, subreddit
    FROM reddit_comments
    WHERE body NOT IN ('[deleted]', '[removed]')
    AND LENGTH(body) > 10
    AND DATE(datetime(created_utc, 'unixepoch')) BETWEEN '2024-01-01' AND '2024-11-30'
    ORDER BY created_utc
"""

df = pd.read_sql(query, conn)
print(f"  ✓ Loaded {len(df):,} total comments")

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

# Create tables if needed
print("\nEnsuring tables exist...")
for dimension in dimensions:
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS reddit_bert_{dimension} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            comment_id TEXT NOT NULL UNIQUE,
            score REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_reddit_bert_{dimension}_comment ON reddit_bert_{dimension}(comment_id)")
conn.commit()

# Load models
base_model_name = "bert-base-uncased"
tokenizer = AutoTokenizer.from_pretrained(base_model_name)
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"  ✓ Using device: {device}")

# Process each dimension
for dimension in dimensions:
    print(f"\n{'='*70}")
    print(f"Processing: {dimension}")
    print(f"{'='*70}")
    
    # Find already-scored comments for THIS dimension
    print("  Checking for existing scores...")
    cur.execute(f"SELECT comment_id FROM reddit_bert_{dimension}")
    already_scored = set(row[0] for row in cur.fetchall())
    print(f"  Already scored: {len(already_scored):,}")
    
    # Filter to unscored comments
    unscored_df = df[~df['id'].isin(already_scored)].copy()
    print(f"  Need to score: {len(unscored_df):,}")
    
    if len(unscored_df) == 0:
        print(f"  ✓ All comments already scored for {dimension}")
        continue
    
    # Load model weights
    print(f"  Loading model weights...")
    model_path = f'models/bert_{dimension}.pt'

    # Load checkpoint first to see what's in it
    checkpoint = torch.load(model_path, map_location=device, weights_only=False)

    # These models were trained for REGRESSION, not classification
    from transformers import BertForSequenceClassification, BertModel
    import torch.nn as nn

    class BertRegressor(nn.Module):
        def __init__(self, bert_model_name):
            super().__init__()
            self.bert = BertModel.from_pretrained(bert_model_name)
            self.regressor = nn.Linear(self.bert.config.hidden_size, 1)
        
        def forward(self, input_ids, attention_mask):
            outputs = self.bert(input_ids=input_ids, attention_mask=attention_mask)
            pooled = outputs.pooler_output
            return self.regressor(pooled)

    model = BertRegressor(base_model_name)
    model.load_state_dict(checkpoint['model_state_dict'])
    model.to(device)
    model.eval()
        
    # Score in batches
    batch_size = 16
    scores = []
    
    print(f"  Scoring {len(unscored_df):,} new comments...")
    
    for i in tqdm(range(0, len(unscored_df), batch_size)):
        batch = unscored_df.iloc[i:i+batch_size]
        texts = batch['body'].tolist()
        
        with torch.no_grad():
            inputs = tokenizer(texts, padding=True, truncation=True, 
                            max_length=512, return_tensors='pt')
            inputs = {k: v.to(device) for k, v in inputs.items()}
            
            try:
                outputs = model(inputs['input_ids'], inputs['attention_mask'])
                # Regression outputs are already scores (not logits)
                batch_scores = outputs.squeeze().cpu().numpy()
                # Ensure it's always an array
                if batch_scores.ndim == 0:
                    batch_scores = [float(batch_scores)]
            except Exception as e:
                print(f"    Error: {e}")
                batch_scores = [0.0] * len(texts)
        
        for comment_id, score in zip(batch['id'], batch_scores):
            scores.append((comment_id, float(score)))
        
        # Commit every 1000 scores
        if len(scores) >= 1000:
            cur.executemany(f"""
                INSERT OR IGNORE INTO reddit_bert_{dimension} (comment_id, score)
                VALUES (?, ?)
            """, scores)
            conn.commit()
            scores = []
    
    # Final commit
    if scores:
        cur.executemany(f"""
            INSERT OR IGNORE INTO reddit_bert_{dimension} (comment_id, score)
            VALUES (?, ?)
        """, scores)
        conn.commit()
    
    print(f"  ✓ {dimension} complete")
    
    # Verify
    cur.execute(f"SELECT COUNT(*) FROM reddit_bert_{dimension}")
    total = cur.fetchone()[0]
    print(f"  Total scores in DB: {total:,}")
    
    del model
    torch.cuda.empty_cache()

conn.close()

print("\n" + "="*70)
print("INCREMENTAL SCORING COMPLETE")
print("="*70)
print("\nRun this script again after more data arrives to score new comments!")