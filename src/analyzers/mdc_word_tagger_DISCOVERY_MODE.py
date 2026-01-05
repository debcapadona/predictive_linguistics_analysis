"""
MDC Word Tagger
Tokenizes stories and tags each word with its story's dimensional classification
"""

import sys
import re
from pathlib import Path
from datetime import datetime
import time

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.database import Database


class WordTagger:
    """Tags words with story-level MDC classifications"""
    
    def __init__(self):
        """Initialize database connections"""
        # SQLite for reading story titles
        self.sqlite_db = Database(db_type='sqlite', db_path='data/linguistic_predictor.db')
        
        # PostgreSQL for reading classifications and writing word tokens
        self.postgres_db = Database(
            db_type='postgresql',
            host='localhost',
            port=5432,
            database='linguistic_predictor_v2',
            user='analyzer',
            password='dev_password_change_in_prod'
        )
    
    def tokenize(self, text: str) -> list:
        """
        Tokenize text into words
        
        Args:
            text: Text to tokenize
            
        Returns:
            List of (word, position) tuples
        """
        if not text or not text.strip():
            return []
        
        # Extract words (alphanumeric sequences)
        words = re.findall(r'\b\w+\b', text)
        
        # Return with positions (1-indexed)
        return [(word, i+1) for i, word in enumerate(words)]
    
    def process_stories(self, limit: int = None):
        """
        Process classified stories and tag their words
        
        Args:
            limit: Optional limit on number of stories to process
        """
        print("MDC Word Tagger")
        print("=" * 80)
        
        # Get classified stories
        query = """
            SELECT sc.story_id, sc.classification_id
            FROM story_classifications sc
            ORDER BY sc.story_id
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        classified_stories = self.postgres_db.execute_query(query)
        total = len(classified_stories)
        
        print(f"\nFound {total} classified stories to process")
        
        if total == 0:
            print("No classified stories found. Run MDC batch processor first.")
            return
        
        # Process stories
        processed = 0
        skipped = 0
        errors = 0
        total_words = 0
        start_time = time.time()
        
        for story_id, classification_id in classified_stories:
            try:
                # Check if already processed
                existing = self.postgres_db.execute_query(
                    "SELECT COUNT(*) FROM word_tokens WHERE story_id = %s",
                    (story_id,)
                )
                
                if existing[0][0] > 0:
                    skipped += 1
                    if skipped % 1000 == 0:
                        print(f"  Skipped {skipped} already-processed stories...")
                    continue
                
                # Get story title from SQLite
                story_result = self.sqlite_db.execute_query(
                    "SELECT title FROM stories WHERE id = ?",
                    (story_id,)
                )
                
                if not story_result or not story_result[0][0]:
                    skipped += 1
                    continue
                
                title = story_result[0][0]
                
                # Tokenize title
                word_tokens = self.tokenize(title)
                
                if not word_tokens:
                    skipped += 1
                    continue
                
                # Insert word tokens
                words_inserted = self.postgres_db.add_word_tokens(
                    story_id=story_id,
                    classification_id=classification_id,
                    words=word_tokens
                )
                
                processed += 1
                total_words += words_inserted
                
                # Progress update
                if processed % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    remaining = (total - processed - skipped) / rate if rate > 0 else 0
                    print(f"  Processed: {processed}/{total} ({processed/total*100:.1f}%) | "
                          f"Words: {total_words:,} | Rate: {rate:.1f}/sec | ETA: {remaining/60:.1f} min")
            
            except Exception as e:
                errors += 1
                if errors <= 10:
                    print(f"  ERROR processing story {story_id}: {str(e)}")
                if errors > 100:
                    print(f"  Too many errors ({errors}), stopping.")
                    break
        
        # Final report
        elapsed = time.time() - start_time
        print("\n" + "=" * 80)
        print(f"Word tagging complete!")
        print(f"  Processed: {processed:,} stories")
        print(f"  Skipped: {skipped:,}")
        print(f"  Errors: {errors}")
        print(f"  Total words tagged: {total_words:,}")
        print(f"  Avg words per story: {total_words/processed:.1f}" if processed > 0 else "")
        print(f"  Time: {elapsed/60:.1f} minutes")
        print(f"  Rate: {processed/elapsed:.1f} stories/sec" if elapsed > 0 else "")
        
        self.close()
    
    def close(self):
        """Close database connections"""
        self.sqlite_db.close()
        self.postgres_db.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Tag words with MDC classifications')
    parser.add_argument('--limit', type=int, help='Limit number of stories to process')
    
    args = parser.parse_args()
    
    tagger = WordTagger()
    tagger.process_stories(limit=args.limit)


if __name__ == "__main__":
    main()