"""
run_bert_metaphor_cluster_density.py - Run trained BERT model on all stories
"""

import torch
import torch.nn as nn
from transformers import BertTokenizer, BertModel
import sqlite3
import psycopg2
from tqdm import tqdm


class BertRegressor(nn.Module):
    """BERT with regression head for score prediction"""
    
    def __init__(self, model_name='bert-base-uncased', dropout=0.1):
        super().__init__()
        self.bert = BertModel.from_pretrained(model_name)
        self.dropout = nn.Dropout(dropout)
        self.regressor = nn.Linear(self.bert.config.hidden_size, 1)
        self.sigmoid = nn.Sigmoid()
    
    def forward(self, input_ids, attention_mask):
        outputs = self.bert(input_ids=input_ids, attention_mask=attention_mask)
        pooled = outputs.pooler_output
        dropped = self.dropout(pooled)
        logits = self.regressor(dropped)
        return self.sigmoid(logits).squeeze()


def load_model():
    """Load trained model"""
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    
    checkpoint = torch.load('models/bert_metaphor_cluster_density.pt', map_location=device, weights_only=False)    
    model = BertRegressor()
    model.load_state_dict(checkpoint['model_state_dict'])
    model.to(device)
    model.eval()
    
    tokenizer = BertTokenizer.from_pretrained(checkpoint['tokenizer_name'])
    
    print(f"Loaded model (val_loss={checkpoint['val_loss']:.4f}, corr={checkpoint['correlation']:.4f})")
    
    return model, tokenizer, device


def get_all_stories():
    """Get all stories from SQLite"""
    conn = sqlite3.connect('data/linguistic_predictor.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT s.id, COALESCE(s.title || ' ' || p.words, s.title) as full_text
        FROM stories s
        LEFT JOIN processed_text p ON s.id = p.story_id
        WHERE s.title IS NOT NULL
    """)
    
    rows = cursor.fetchall()
    conn.close()
    
    return [(r[0], r[1]) for r in rows if r[1]]


def run_inference(model, tokenizer, device, stories, batch_size=32):
    """Run inference on all stories"""
    
    results = []
    
    for i in tqdm(range(0, len(stories), batch_size), desc="Scoring"):
        batch = stories[i:i+batch_size]
        story_ids = [s[0] for s in batch]
        texts = [s[1][:512] for s in batch]  # Truncate
        
        encoding = tokenizer(
            texts,
            truncation=True,
            padding=True,
            max_length=128,
            return_tensors='pt'
        )
        
        with torch.no_grad():
            input_ids = encoding['input_ids'].to(device)
            attention_mask = encoding['attention_mask'].to(device)
            scores = model(input_ids, attention_mask)
        
        if scores.dim() == 0:
            scores = scores.unsqueeze(0)
        
        for story_id, score in zip(story_ids, scores.cpu().numpy()):
            results.append((story_id, float(score)))
    
    return results


def save_to_postgres(results):
    """Save BERT scores to PostgreSQL"""
    
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="linguistic_predictor_v2",
        user="analyzer",
        password="dev_password_change_in_prod"
    )
    cursor = conn.cursor()
    
    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bert_metaphor_cluster_density (
            story_id TEXT PRIMARY KEY,
            score REAL NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    # Insert scores
    inserted = 0
    for story_id, score in tqdm(results, desc="Saving to PostgreSQL"):
        cursor.execute("""
            INSERT INTO bert_metaphor_cluster_density (story_id, score)
            VALUES (%s, %s)
            ON CONFLICT (story_id) DO UPDATE SET score = %s
        """, (story_id, score, score))
        inserted += 1
        
        if inserted % 10000 == 0:
            conn.commit()
    
    conn.commit()
    conn.close()
    
    print(f"Saved {inserted} scores to PostgreSQL")


if __name__ == "__main__":
    print("="*60)
    print("BERT Metaphor Density - Full Inference")
    print("="*60)
    
    # Load model
    model, tokenizer, device = load_model()
    
    # Get stories
    print("\nLoading stories from SQLite...")
    stories = get_all_stories()
    print(f"Found {len(stories)} stories")
    
    # Run inference
    print("\nRunning inference...")
    results = run_inference(model, tokenizer, device, stories, batch_size=32)
    
    # Save
    print("\nSaving results...")
    save_to_postgres(results)
    
    # Summary stats
    scores = [r[1] for r in results]
    print(f"\n{'='*60}")
    print(f"COMPLETE")
    print(f"{'='*60}")
    print(f"Stories scored: {len(results)}")
    print(f"Score range: {min(scores):.3f} - {max(scores):.3f}")
    print(f"Score mean: {sum(scores)/len(scores):.3f}")
    print(f"{'='*60}")