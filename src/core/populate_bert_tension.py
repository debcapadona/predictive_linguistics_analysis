"""
populate_bert_tension.py - Populate BERT tension scores for all stories/comments

Reads from main db, runs BERT analyzer, writes to experiment db.
Designed for overnight runs.
"""

import sys
import os
import sqlite3
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.analyzers.tension_analyzer_bert import TensionAnalyzerBERT


class BertTensionPopulator:
    """Populate BERT tension scores"""
    
    def __init__(self, 
                 source_db: str = "data/linguistic_predictor.db",
                 target_db: str = "data/bert_experiments.db"):
        """
        Initialize populator
        
        Args:
            source_db: Path to main database with stories
            target_db: Path to experiment database for results
        """
        self.source_conn = sqlite3.connect(source_db)
        self.target_conn = sqlite3.connect(target_db)
        self.analyzer = TensionAnalyzerBERT()
    
    def get_unprocessed_count(self) -> int:
        """Get count of records not yet processed"""
        cursor = self.source_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM stories s
            WHERE s.id NOT IN (
                SELECT story_id FROM bert_tension_scores
            )
        """.replace("FROM bert_tension_scores", 
                    f"FROM '{self.target_conn.execute('PRAGMA database_list').fetchone()[2]}'.bert_tension_scores"))
        
        # Simpler approach - just count source and target separately
        source_count = self.source_conn.execute("SELECT COUNT(*) FROM stories").fetchone()[0]
        target_count = self.target_conn.execute("SELECT COUNT(*) FROM bert_tension_scores").fetchone()[0]
        return source_count - target_count
    
    def populate(self, batch_size: int = 50, limit: int = None):
        """
        Run BERT analyzer on all unprocessed stories
        
        Args:
            batch_size: Commit every N records
            limit: Max records to process (None = all)
        """
        import gc
        
        print("=" * 70)
        print("BERT TENSION POPULATOR")
        print("=" * 70)
        
        # Get already processed IDs
        processed_ids = set(
            row[0] for row in 
            self.target_conn.execute("SELECT story_id FROM bert_tension_scores").fetchall()
        )
        print(f"Already processed: {len(processed_ids)}")
        
        # Get total count of records with text
        total_available = self.source_conn.execute("""
            SELECT COUNT(*) FROM stories s
            JOIN processed_text p ON s.id = p.story_id
            WHERE p.words IS NOT NULL AND p.words != ''
        """).fetchone()[0]
        print(f"Total available: {total_available}")
        
        remaining_estimate = total_available - len(processed_ids)
        print(f"Remaining to process: ~{remaining_estimate}")
        
        # Get stories to process - exclude already processed, only get records with text
        processed_list = ','.join(f"'{pid}'" for pid in processed_ids) if processed_ids else "''"
        
        query = f"""
            SELECT s.id, s.title, s.created_at, s.content_type, p.words
            FROM stories s
            JOIN processed_text p ON s.id = p.story_id
            WHERE s.id NOT IN ({processed_list})
              AND p.words IS NOT NULL 
              AND p.words != ''
            ORDER BY s.created_at
        """
        if limit:
            query += f" LIMIT {limit}"
        
        cursor = self.source_conn.cursor()
        cursor.execute(query)
        
        processed = 0
        skipped = 0
        errors = 0
        start_time = datetime.now()
        
        print(f"Starting at {start_time.strftime('%H:%M:%S')}")
        print("-" * 70)
        
        for row in cursor:
            story_id, title, created_at, content_type, words = row
            
            # Convert pipe-delimited words back to text
            text = ' '.join(words.split('|'))
            
            try:
                # Run BERT analyzer
                result = self.analyzer.score(text)
                
                # Insert into target db
                self.target_conn.execute("""
                    INSERT INTO bert_tension_scores 
                    (story_id, created_at, sentiment, sentiment_score, confidence,
                     dominant_emotion, anger, disgust, fear, joy, neutral, sadness, surprise)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    story_id,
                    created_at,
                    result['sentiment'],
                    result['sentiment_score'],
                    result['confidence'],
                    result['dominant_emotion'],
                    result['emotions'].get('anger', 0),
                    result['emotions'].get('disgust', 0),
                    result['emotions'].get('fear', 0),
                    result['emotions'].get('joy', 0),
                    result['emotions'].get('neutral', 0),
                    result['emotions'].get('sadness', 0),
                    result['emotions'].get('surprise', 0)
                ))
                
                processed += 1
                
                # Commit in batches and force garbage collection
                if processed % batch_size == 0:
                    self.target_conn.commit()
                    gc.collect()
                    
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = processed / elapsed if elapsed > 0 else 0
                    remaining = remaining_estimate - processed
                    eta_min = remaining / rate / 60 if rate > 0 else 0
                    
                    print(f"  Processed: {processed} | Skipped: {skipped} | "
                          f"Rate: {rate:.1f}/sec | ETA: {eta_min:.0f} min")
                
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  Error on {story_id}: {e}")
        
        # Final commit
        self.target_conn.commit()
        gc.collect()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        print("=" * 70)
        print(f"Complete!")
        print(f"  Processed: {processed}")
        print(f"  Skipped: {skipped}")
        print(f"  Errors: {errors}")
        print(f"  Time: {elapsed/60:.1f} minutes")
        if elapsed > 0:
            print(f"  Rate: {processed/elapsed:.1f}/sec")
        print("=" * 70)
        
        for row in cursor:
            story_id, title, created_at, content_type, words = row
            
            # Skip if already processed
            if story_id in processed_ids:
                skipped += 1
                continue
            
            # Convert pipe-delimited words back to text
            text = ' '.join(words.split('|'))
            
            try:
                # Run BERT analyzer
                result = self.analyzer.score(text)
                
                # Insert into target db
                self.target_conn.execute("""
                    INSERT INTO bert_tension_scores 
                    (story_id, created_at, sentiment, sentiment_score, confidence,
                     dominant_emotion, anger, disgust, fear, joy, neutral, sadness, surprise)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    story_id,
                    created_at,
                    result['sentiment'],
                    result['sentiment_score'],
                    result['confidence'],
                    result['dominant_emotion'],
                    result['emotions'].get('anger', 0),
                    result['emotions'].get('disgust', 0),
                    result['emotions'].get('fear', 0),
                    result['emotions'].get('joy', 0),
                    result['emotions'].get('neutral', 0),
                    result['emotions'].get('sadness', 0),
                    result['emotions'].get('surprise', 0)
                ))
                
                processed += 1
                
                # Commit in batches
                if processed % batch_size == 0:
                    self.target_conn.commit()
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = processed / elapsed if elapsed > 0 else 0
                    total = processed + skipped + len(processed_ids)
                    remaining = 197496 - total
                    eta_min = remaining / rate / 60 if rate > 0 else 0
                    
                    print(f"  Processed: {processed} | Skipped: {skipped} | "
                          f"Rate: {rate:.1f}/sec | ETA: {eta_min:.0f} min")
                
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  Error on {story_id}: {e}")
        
        # Final commit
        self.target_conn.commit()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        print("=" * 70)
        print(f"Complete!")
        print(f"  Processed: {processed}")
        print(f"  Skipped: {skipped}")
        print(f"  Errors: {errors}")
        print(f"  Time: {elapsed/60:.1f} minutes")
        print(f"  Rate: {processed/elapsed:.1f}/sec" if elapsed > 0 else "")
        print("=" * 70)
    
    def close(self):
        """Close connections"""
        self.source_conn.close()
        self.target_conn.close()


if __name__ == "__main__":
    populator = BertTensionPopulator()
    
    # Optional: pass limit for testing
    # populator.populate(limit=100)
    
    # Full run
    populator.populate()
    
    populator.close()