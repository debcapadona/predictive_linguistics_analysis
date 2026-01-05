"""
Data Loader - Copy data from SQLite to PostgreSQL
Loads in dependency order: stories → comments → enrichments
"""

import sys
from pathlib import Path
from datetime import datetime
import time

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.database import Database


def load_stories(sqlite_db, postgres_db):
    """Load stories from SQLite to PostgreSQL"""
    print("\n" + "=" * 80)
    print("Loading Stories")
    print("=" * 80)
    
    # Get stories from SQLite
    stories = sqlite_db.execute_query("""
        SELECT id, title, url, created_at, content_type, author
        FROM stories
        ORDER BY id
    """)
    
    total = len(stories)
    print(f"Found {total:,} stories in SQLite")
    
    if total == 0:
        print("No stories to load")
        return 0
    
    # Insert into PostgreSQL in batches
    batch_size = 1000
    loaded = 0
    start_time = time.time()
    
    for i in range(0, total, batch_size):
        batch = stories[i:i+batch_size]
        
        for story in batch:
            try:
                postgres_db.execute_update("""
                    INSERT INTO stories (id, title, url, created_at, content_type, author, score, num_comments)
                    VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL)
                    ON CONFLICT (id) DO NOTHING
                """, story)
                loaded += 1
            except Exception as e:
                print(f"  Error loading story {story[0]}: {e}")
        
        if (i + batch_size) % 10000 == 0:
            elapsed = time.time() - start_time
            rate = loaded / elapsed if elapsed > 0 else 0
            print(f"  Loaded: {loaded:,}/{total:,} ({loaded/total*100:.1f}%) | Rate: {rate:.0f}/sec")
    
    elapsed = time.time() - start_time
    print(f"✓ Loaded {loaded:,} stories in {elapsed:.1f} seconds")
    return loaded


def load_comments(sqlite_db, postgres_db):
    """Load comments from SQLite to PostgreSQL"""
    print("\n" + "=" * 80)
    print("Loading Comments")
    print("=" * 80)
    
    try:
        # Get comments from SQLite
        comments = sqlite_db.execute_query("""
            SELECT id, story_id, parent_id, author, text, created_at
            FROM comments
            ORDER BY id
        """)
    except Exception as e:
        print(f"No comments table found in SQLite: {e}")
        print("Skipping comments load")
        return 0
    
    total = len(comments)
    print(f"Found {total:,} comments in SQLite")
    
    if total == 0:
        print("No comments to load")
        return 0
    
    # Insert into PostgreSQL in batches
    batch_size = 1000
    loaded = 0
    start_time = time.time()
    
    for i in range(0, total, batch_size):
        batch = comments[i:i+batch_size]
        
        for comment in batch:
            try:
                postgres_db.execute_update("""
                    INSERT INTO comments (id, story_id, parent_id, author, text, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, comment)
                loaded += 1
            except Exception as e:
                print(f"  Error loading comment {comment[0]}: {e}")
        
        if (i + batch_size) % 50000 == 0:
            elapsed = time.time() - start_time
            rate = loaded / elapsed if elapsed > 0 else 0
            print(f"  Loaded: {loaded:,}/{total:,} ({loaded/total*100:.1f}%) | Rate: {rate:.0f}/sec")
    
    elapsed = time.time() - start_time
    print(f"✓ Loaded {loaded:,} comments in {elapsed:.1f} seconds")
    return loaded


def main():
    """Load all data from SQLite to PostgreSQL"""
    print("Data Loader: SQLite → PostgreSQL")
    print("=" * 80)
    
    # Connect to databases
    sqlite_db = Database(db_type='sqlite', db_path='data/linguistic_predictor.db')
    postgres_db = Database(
        db_type='postgresql',
        host='localhost',
        port=5432,
        database='linguistic_predictor_v2',
        user='analyzer',
        password='dev_password_change_in_prod'
    )
    
    start_time = time.time()
    
    # Load in dependency order
    stories_loaded = load_stories(sqlite_db, postgres_db)
    comments_loaded = load_comments(sqlite_db, postgres_db)
    
    # Summary
    total_time = time.time() - start_time
    print("\n" + "=" * 80)
    print("Data Load Complete!")
    print("=" * 80)
    print(f"Stories loaded: {stories_loaded:,}")
    print(f"Comments loaded: {comments_loaded:,}")
    print(f"Total time: {total_time/60:.1f} minutes")
    print(f"Overall rate: {(stories_loaded + comments_loaded)/total_time:.0f} records/sec")
    
    sqlite_db.close()
    postgres_db.close()


if __name__ == "__main__":
    main()