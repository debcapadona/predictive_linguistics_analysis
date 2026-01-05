"""
Apply sigmoid transformation to Reddit BERT scores
Converts raw regression outputs (-inf to +inf) to probabilities (0 to 1)
"""
import sqlite3
import numpy as np
from tqdm import tqdm

def sigmoid(x):
    """Convert raw score to 0-1 probability"""
    return 1 / (1 + np.exp(-np.clip(x, -500, 500)))  # Clip to prevent overflow

print("="*70)
print("FIXING REDDIT BERT SCORES (APPLYING SIGMOID)")
print("="*70)

conn = sqlite3.connect('data/reddit_snapshot_dec29.db')
cur = conn.cursor()

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

for dimension in dimensions:
    print(f"\nProcessing: {dimension}")
    
    # Get all scores
    cur.execute(f"SELECT id, score FROM reddit_bert_{dimension}")
    rows = cur.fetchall()
    print(f"  Loaded {len(rows):,} scores")
    
    # Apply sigmoid
    updates = []
    for row_id, raw_score in tqdm(rows):
        transformed_score = float(sigmoid(raw_score))
        updates.append((transformed_score, row_id))
    
    # Update in batches
    print(f"  Updating scores...")
    batch_size = 10000
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i+batch_size]
        cur.executemany(f"""
            UPDATE reddit_bert_{dimension}
            SET score = ?
            WHERE id = ?
        """, batch)
        conn.commit()
    
    # Verify
    cur.execute(f"""
        SELECT MIN(score), MAX(score), AVG(score) 
        FROM reddit_bert_{dimension}
    """)
    min_s, max_s, avg_s = cur.fetchone()
    print(f"  ✓ New range: {min_s:.4f} to {max_s:.4f} (avg: {avg_s:.4f})")

conn.close()

print("\n" + "="*70)
print("SCORES FIXED")
print("="*70)
print("\nAll Reddit BERT scores now in 0-1 range (matching HN)")
print("Re-run comparison script to see corrected results!")