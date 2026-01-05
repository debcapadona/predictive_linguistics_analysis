"""
load_data_to_db.py - Load CSV data into database

Import processed CSV files into SQLite database
"""

import csv
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.database import Database


class DataLoader:
    """Load CSV data into database"""
    
    def __init__(self, db_path: str = "data/linguistic_predictor.db"):
        """
        Initialize loader
        
        Args:
            db_path: Path to database
        """
        self.db = Database(db_type='sqlite', db_path=db_path)
    
    def load_processed_csv(self, csv_path: str, source_name: str, source_type: str):
        """
        Load a processed CSV file into database
        
        Args:
            csv_path: Path to processed CSV
            source_name: Name of source (e.g., "Hacker News May-July 2024")
            source_type: Type of source (e.g., "hackernews", "rss")
        """
        print(f"\n{'='*80}")
        print(f"Loading: {csv_path}")
        print(f"Source: {source_name} ({source_type})")
        print(f"{'='*80}")
        
        # Add source to database
        source_id = self.db.add_source(source_name, source_type)
        print(f"✓ Source ID: {source_id}")
        
        # Read CSV
        stories_loaded = 0
        stories_skipped = 0
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # Prepare story data
                story = {
                    'id': row.get('id', f"{source_type}_{row.get('created_at', '')}"),
                    'content_type': 'header',  # This is a header/title
                    'title': row.get('original_title', ''),
                    'url': row.get('url', ''),
                    'created_at': row.get('created_at', ''),
                    'parent_story_id': row.get('story_id', None),
                    'author': row.get('author', None),
                    'comment_depth': row.get('depth', None)
                }
                
                try:
                    self.db.add_story(story, source_id)
                    stories_loaded += 1
                except Exception as e:
                    if stories_skipped < 5:  # Print first 5 errors
                        print(f"\n⚠️  Error: {e}")
                        print(f"   Story ID: {story.get('id')}")
                    stories_skipped += 1
                    continue
                
                # Add processed text
                processed = {
                    'words': row.get('words', ''),
                    'bigrams': row.get('bigrams', ''),
                    'trigrams': row.get('trigrams', ''),
                    'word_count': int(row.get('word_count', 0))
                }
                
                try:
                    self.db.add_processed_text(story['id'], processed)
                except Exception as e:
                    pass
                
                # Progress indicator
                if stories_loaded % 100 == 0:
                    print(f"  Loaded {stories_loaded} stories...", end='\r')
        
        # Commit changes
        self.db.conn.commit()
        
        print(f"\n✓ Loaded {stories_loaded} stories")
        if stories_skipped > 0:
            print(f"  Skipped {stories_skipped} duplicates")
    
    def show_stats(self):
        """Display database statistics"""
        print(f"\n{'='*80}")
        print("DATABASE STATISTICS")
        print(f"{'='*80}")
        
        stats = self.db.get_stats()
        
        print(f"\nTotal stories: {stats['total_stories']}")
        print(f"Total processed: {stats['total_processed']}")
        
        print(f"\nStories by source:")
        for source in stats['by_source']:
            print(f"  {source['name']:40s} ({source['type']:12s}): {source['count']:5d} stories")
        
        if stats['date_range']['earliest'] and stats['date_range']['latest']:
            print(f"\nDate range:")
            print(f"  Earliest: {stats['date_range']['earliest']}")
            print(f"  Latest:   {stats['date_range']['latest']}")
        
        print(f"{'='*80}")
    
    def close(self):
        """Close database connection"""
        self.db.close()


# Main execution
if __name__ == "__main__":
    print("="*80)
    print("DATA LOADER - CSV TO DATABASE")
    print("="*80)
    
    loader = DataLoader()
    
# Load 2024 full year
hn_2024 = "data/processed/hackernews/hn_2024_full_year_processed.csv"
if os.path.exists(hn_2024):
    loader.load_processed_csv(
        hn_2024,
        "Hacker News 2024 Full Year",
        "hackernews"
    )

# Load 2025 YTD
hn_2025 = "data/processed/hackernews/hn_2025_ytd_processed.csv"
if os.path.exists(hn_2025):
    loader.load_processed_csv(
        hn_2025,
        "Hacker News 2025 YTD",
        "hackernews"
    )

    # Load HackerNews May-July 2024
    hn_file = "data/processed/hackernews/hn_may_july_2024_processed.csv"
    if os.path.exists(hn_file):
        loader.load_processed_csv(
            hn_file,
            "Hacker News May-July 2024",
            "hackernews"
        )
    else:
        print(f"\n⚠️  File not found: {hn_file}")
    
    # Load RSS Feeds
    rss_file = "data/processed/rss/rss_feeds_latest_processed.csv"
    if os.path.exists(rss_file):
        loader.load_processed_csv(
            rss_file,
            "RSS Feeds Latest",
            "rss"
        )
    else:
        print(f"\n⚠️  File not found: {rss_file}")
    
    # Load HackerNews Nov test data if it exists
    hn_test_file = "data/processed/hackernews/hn_test_nov2024_processed.csv"
    if os.path.exists(hn_test_file):
        loader.load_processed_csv(
            hn_test_file,
            "Hacker News Nov 2024 Test",
            "hackernews"
        )
    
    # Show final stats
    loader.show_stats()
    
    loader.close()
    
    print("\n✅ Data loading complete!")
    print("Database location: data/linguistic_predictor.db")