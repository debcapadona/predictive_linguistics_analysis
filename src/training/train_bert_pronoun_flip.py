"""
train_bert_pronoun_flip.py - Fine-tune BERT to predict pronoun flip scores

Uses zero-shot labels as training data.
"""

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from transformers import BertTokenizer, BertModel, get_linear_schedule_with_warmup
from torch.optim import AdamW
import psycopg2
import numpy as np
from sklearn.model_selection import train_test_split
from tqdm import tqdm
import json


class TimeCompressionDataset(Dataset):
    """Dataset for certainty pronoun flip regression"""
    
    def __init__(self, texts, scores, tokenizer, max_length=128):
        self.texts = texts
        self.scores = scores
        self.tokenizer = tokenizer
        self.max_length = max_length
    
    def __len__(self):
        return len(self.texts)
    
    def __getitem__(self, idx):
        text = self.texts[idx]
        score = self.scores[idx]
        
        encoding = self.tokenizer(
            text,
            truncation=True,
            padding='max_length',
            max_length=self.max_length,
            return_tensors='pt'
        )
        
        return {
            'input_ids': encoding['input_ids'].squeeze(),
            'attention_mask': encoding['attention_mask'].squeeze(),
            'score': torch.tensor(score, dtype=torch.float)
        }


class BertRegressor(nn.Module):
    """BERT with regression head for score prediction"""
    
    def __init__(self, model_name='bert-base-uncased', dropout=0.1):
        super().__init__()
        self.bert = BertModel.from_pretrained(model_name)
        self.dropout = nn.Dropout(dropout)
        self.regressor = nn.Linear(self.bert.config.hidden_size, 1)
        self.sigmoid = nn.Sigmoid()  # Constrain output to 0-1
    
    def forward(self, input_ids, attention_mask):
        outputs = self.bert(input_ids=input_ids, attention_mask=attention_mask)
        pooled = outputs.pooler_output
        dropped = self.dropout(pooled)
        logits = self.regressor(dropped)
        return self.sigmoid(logits).squeeze()


def load_training_data():
    """Load texts and scores from PostgreSQL + SQLite"""
    import sqlite3
    
    # Get scores from PostgreSQL
    pg_conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="linguistic_predictor_v2",
        user="analyzer",
        password="dev_password_change_in_prod"
    )
    pg_cursor = pg_conn.cursor()
    
    pg_cursor.execute("""
        SELECT story_id, pronoun_flip 
        FROM zero_shot_labels 
        WHERE pronoun_flip IS NOT NULL
    """)
    
    score_map = {row[0]: row[1] for row in pg_cursor.fetchall()}
    pg_conn.close()
    
    # Get text from SQLite
    sqlite_conn = sqlite3.connect('data/linguistic_predictor.db')
    sqlite_cursor = sqlite_conn.cursor()
    
    story_ids = list(score_map.keys())
    placeholders = ','.join(['?' for _ in story_ids])
    
    sqlite_cursor.execute(f"""
        SELECT s.id, COALESCE(s.title || ' ' || p.words, s.title) as full_text
        FROM stories s
        LEFT JOIN processed_text p ON s.id = p.story_id
        WHERE s.id IN ({placeholders})
    """, story_ids)
    
    rows = sqlite_cursor.fetchall()
    sqlite_conn.close()
    
    # Combine
    texts = []
    scores = []
    for story_id, text in rows:
        if text and story_id in score_map:
            texts.append(text)
            scores.append(score_map[story_id])
    
    print(f"Loaded {len(texts)} training examples")
    print(f"Score range: {min(scores):.3f} - {max(scores):.3f}")
    print(f"Score mean: {np.mean(scores):.3f}")
    
    return texts, scores


def train_model(texts, scores, epochs=3, batch_size=16, lr=2e-5):
    """Fine-tune BERT on pronoun flip scores"""
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")
    
    # Split data
    train_texts, val_texts, train_scores, val_scores = train_test_split(
        texts, scores, test_size=0.1, random_state=42
    )
    print(f"Train: {len(train_texts)}, Val: {len(val_texts)}")
    
    # Tokenizer and datasets
    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
    
    train_dataset = TimeCompressionDataset(train_texts, train_scores, tokenizer)
    val_dataset = TimeCompressionDataset(val_texts, val_scores, tokenizer)
    
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=batch_size)
    
    # Model
    model = BertRegressor().to(device)
    
    # Optimizer and scheduler
    optimizer = AdamW(model.parameters(), lr=lr)
    total_steps = len(train_loader) * epochs
    scheduler = get_linear_schedule_with_warmup(
        optimizer, 
        num_warmup_steps=total_steps // 10,
        num_training_steps=total_steps
    )
    
    # Loss function
    criterion = nn.MSELoss()
    
    # Training loop
    best_val_loss = float('inf')
    
    for epoch in range(epochs):
        # Train
        model.train()
        train_loss = 0
        
        for batch in tqdm(train_loader, desc=f"Epoch {epoch+1}/{epochs}"):
            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)
            scores_batch = batch['score'].to(device)
            
            optimizer.zero_grad()
            outputs = model(input_ids, attention_mask)
            loss = criterion(outputs, scores_batch)
            loss.backward()
            
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            scheduler.step()
            
            train_loss += loss.item()
        
        avg_train_loss = train_loss / len(train_loader)
        
        # Validate
        model.eval()
        val_loss = 0
        predictions = []
        actuals = []
        
        with torch.no_grad():
            for batch in val_loader:
                input_ids = batch['input_ids'].to(device)
                attention_mask = batch['attention_mask'].to(device)
                scores_batch = batch['score'].to(device)
                
                outputs = model(input_ids, attention_mask)
                loss = criterion(outputs, scores_batch)
                val_loss += loss.item()
                
                predictions.extend(outputs.cpu().numpy())
                actuals.extend(scores_batch.cpu().numpy())
        
        avg_val_loss = val_loss / len(val_loader)
        
        # Correlation
        correlation = np.corrcoef(predictions, actuals)[0, 1]
        
        print(f"Epoch {epoch+1}: Train Loss={avg_train_loss:.4f}, Val Loss={avg_val_loss:.4f}, Corr={correlation:.4f}")
        
        # Save best model
        if avg_val_loss < best_val_loss:
            best_val_loss = avg_val_loss
            torch.save({
                'model_state_dict': model.state_dict(),
                'tokenizer_name': 'bert-base-uncased',
                'val_loss': best_val_loss,
                'correlation': correlation
            }, 'models/bert_pronoun_flip.pt')
            print(f"  Saved best model (val_loss={best_val_loss:.4f})")
    
    return model, tokenizer


def test_model(model, tokenizer, device):
    """Test on sample texts"""
    
    test_texts = [
        "Normal tech news about a product launch",
        "Everything is happening so fast I can't keep up",
        "AI is accelerating at unprecedented pace - feels like years compressed into weeks",
        "The market is moving too fast, overwhelming amount of information",
        "Exponentially faster development, time is speeding up, can barely process it all"
    ]
    
    model.eval()
    print("\n" + "="*60)
    print("TEST PREDICTIONS")
    print("="*60)
    
    for text in test_texts:
        encoding = tokenizer(
            text,
            truncation=True,
            padding='max_length',
            max_length=128,
            return_tensors='pt'
        )
        
        with torch.no_grad():
            input_ids = encoding['input_ids'].to(device)
            attention_mask = encoding['attention_mask'].to(device)
            score = model(input_ids, attention_mask).item()
        
        print(f"\nText: {text[:60]}...")
        print(f"Predicted pronoun_flip: {score:.3f}")


if __name__ == "__main__":
    import os
    os.makedirs('models', exist_ok=True)
    
    print("="*60)
    print("BERT Pronoun Flip Training")
    print("="*60)
    
    # Load data
    texts, scores = load_training_data()
    
    # Train
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model, tokenizer = train_model(texts, scores, epochs=3, batch_size=16)
    
    # Test
    model = model.to(device)
    test_model(model, tokenizer, device)
    
    print("\n" + "="*60)
    print("Training complete! Model saved to models/bert_pronoun_flip.pt")
    print("="*60)