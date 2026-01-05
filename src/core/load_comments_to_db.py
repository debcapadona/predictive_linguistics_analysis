"""
load_comments_to_db.py - Load comment CSV data into database

Import processed comment CSV files into SQLite database
"""

import csv
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.database import Database


class CommentLoader:
    """Load comment CSV data into database"""
    
    def __init__(self, db_path: str = "data/linguistic_predictor.db"):
        """Initialize loader"""
        self.db = Database(db_type='sqlite', db_path=db_path)
    
    def load_processed_comments_csv(self, csv_path: str, source_name: str):
        """
        Load a processed comments CSV file into database
        
        Args:
            csv_path: Path to processed comments CSV
            source_name: Name of source
        """
        print(f"\n{'='*80}")
        print(f"Loading: {csv_path}")
        print(f"Source: {source_name}")
        print(f"{'='*80}")
        
        # Add source to database
        source_id = self.db.add_source(source_name, "hackernews")
        print(f"✓ Source ID: {source_id}")
        
        # Read CSV
        comments_loaded = 0
        comments_skipped = 0
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # Prepare comment data
                comment = {
                    'id': row.get('id', ''),
                    'content_type': 'comment',  # This is a comment
                    'title': row.get('text', '')[:200],  # Store first 200 chars as "title" for consistency
                    'url': '',  # Comments don't have URLs
                    'created_at': row.get('created_at', ''),
                    'parent_story_id': row.get('story_id', ''),
                    'author': row.get('author', ''),
                    'comment_depth': int(row.get('depth', 0))
                }
                
                # Add comment as story
                try:
                    self.db.add_story(comment, source_id)
                    comments_loaded += 1
                except Exception as e:
                    comments_skipped += 1
                    continue
                
                # Add processed text
                processed = {
                    'words': row.get('words', ''),
                    'bigrams': row.get('bigrams', ''),
                    'trigrams': row.get('trigrams', ''),
                    'word_count': int(row.get('word_count', 0))
                }
                
                try:
                    self.db.add_processed_text(comment['id'], processed)
                except Exception as e:
                    pass
                
                # Progress indicator
                if comments_loaded % 1000 == 0:
                    print(f"  Loaded {comments_loaded} comments...", end='\r')
        
        # Commit changes
        self.db.conn.commit()
        
        print(f"\n✓ Loaded {comments_loaded} comments")
        if comments_skipped > 0:
            print(f"  Skipped {comments_skipped} duplicates")
    
    def close(self):
        """Close database connection"""
        self.db.close()


# Main execution
if __name__ == "__main__":
    print("="*80)
    print("COMMENT LOADER - CSV TO DATABASE")
    print("="*80)
    
    loader = CommentLoader()
    
    # Load 2024 comments
    comments_2024 = "data/processed/hackernews/hn_2024_full_year_comments_processed.csv"
    if os.path.exists(comments_2024):
        loader.load_processed_comments_csv(
            comments_2024,
            "HN 2024 Comments"
        )
    else:
        print(f"\n⚠️  File not found: {comments_2024}")
    
    # Load 2025 comments
    comments_2025 = "data/processed/hackernews/hn_2025_ytd_comments_processed.csv"
    if os.path.exists(comments_2025):
        loader.load_processed_comments_csv(
            comments_2025,
            "HN 2025 Comments"
        )
    else:
        print(f"\n⚠️  File not found: {comments_2025}")
    
    # Show stats
    print(f"\n{'='*80}")
    print("DATABASE STATISTICS")
    print(f"{'='*80}")
    stats = loader.db.get_stats()
    print(f"\nTotal records: {stats['total_stories']}")
    print(f"By source:")
    for source in stats['by_source']:
        print(f"  {source['name']:30s}: {source['count']:7d} records")
    print(f"{'='*80}")
    
    loader.close()