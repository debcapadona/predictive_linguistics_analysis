"""
populate_word_bert_tags.py - Tag each word with its story's BERT scores
"""

import re
import sqlite3
import psycopg2
from tqdm import tqdm


def tokenize(text):
    """Split text into words with positions"""
    if not text:
        return []
    words = re.findall(r'\b\w+\b', text)
    return [(word, word.lower(), i+1) for i, word in enumerate(words)]


def main():
    print("="*60)
    print("Word BERT Tagger")
    print("="*60)
    
    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="linguistic_predictor_v2",
        user="analyzer",
        password="dev_password_change_in_prod"
    )
    pg_cursor = pg_conn.cursor()
    
    # Connect to SQLite
    sqlite_conn = sqlite3.connect('data/linguistic_predictor.db')
    sqlite_cursor = sqlite_conn.cursor()
    
    # Get all BERT scores
    print("\nLoading BERT scores...")
    pg_cursor.execute("SELECT * FROM story_bert_scores")
    scores = {row[0]: row[1:] for row in pg_cursor.fetchall()}
    print(f"Loaded scores for {len(scores)} stories")
    
    # Get story texts from SQLite
    print("\nLoading story texts...")
    story_ids = list(scores.keys())
    placeholders = ','.join(['?' for _ in story_ids])
    
    sqlite_cursor.execute(f"""
        SELECT s.id, COALESCE(s.title || ' ' || p.words, s.title) as full_text
        FROM stories s
        LEFT JOIN processed_text p ON s.id = p.story_id
        WHERE s.id IN ({placeholders})
    """, story_ids)
    
    stories = {row[0]: row[1] for row in sqlite_cursor.fetchall() if row[1]}
    print(f"Loaded {len(stories)} story texts")
    
    # Process and insert
    print("\nTagging words...")
    batch = []
    batch_size = 10000
    total_words = 0
    
    for story_id, text in tqdm(stories.items(), desc="Processing"):
        if story_id not in scores:
            continue
            
        score_tuple = scores[story_id]
        tokens = tokenize(text)
        
        for word, word_lower, position in tokens:
            batch.append((
                story_id, word, word_lower, position,
                score_tuple[0],  # time_compression
                score_tuple[1],  # temporal_bleed
                score_tuple[2],  # certainty_collapse
                score_tuple[3],  # emotional_valence
                score_tuple[4],  # agency_reversal
                score_tuple[5],  # novel_meme
                score_tuple[6],  # metaphor_density
                score_tuple[7],  # pronoun_flip
                score_tuple[8],  # sacred_profane
            ))
            
            if len(batch) >= batch_size:
                pg_cursor.executemany("""
                    INSERT INTO word_bert_tags 
                    (story_id, word_text, word_lower, position,
                     time_compression, temporal_bleed, certainty_collapse,
                     emotional_valence, agency_reversal, novel_meme,
                     metaphor_density, pronoun_flip, sacred_profane)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                pg_conn.commit()
                total_words += len(batch)
                batch = []
    
    # Insert remaining
    if batch:
        pg_cursor.executemany("""
            INSERT INTO word_bert_tags 
            (story_id, word_text, word_lower, position,
             time_compression, temporal_bleed, certainty_collapse,
             emotional_valence, agency_reversal, novel_meme,
             metaphor_density, pronoun_flip, sacred_profane)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)
        pg_conn.commit()
        total_words += len(batch)
    
    print(f"\n{'='*60}")
    print(f"COMPLETE: Tagged {total_words} words")
    print(f"{'='*60}")
    
    pg_conn.close()
    sqlite_conn.close()


if __name__ == "__main__":
    main()