"""
Fixed Reddit comment scraper - targets specific date range only
"""
import requests
import time
import sqlite3
from datetime import datetime, timedelta

print("="*70)
print("REDDIT COMMENT SCRAPER (FIXED)")
print("="*70)

DB_PATH = 'data/reddit_data.db'
DELAY = 0.6  # seconds between requests (100/min)

# Target June 8-15, 2024 (Reddit API Blackout validation)
TARGET_START = datetime(2024, 6, 8)
TARGET_END = datetime(2024, 6, 15)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Get posts from target date range
print(f"\nGetting posts from {TARGET_START.date()} to {TARGET_END.date()}...")
cur.execute("""
    SELECT id, subreddit, created_utc 
    FROM reddit_posts 
    WHERE created_utc BETWEEN ? AND ?
    ORDER BY created_utc
""", (int(TARGET_START.timestamp()), int(TARGET_END.timestamp())))

posts = cur.fetchall()
print(f"Found {len(posts)} posts to process")

# Scrape comments
comments_saved = 0
errors = 0

for idx, (post_id, subreddit, created_utc) in enumerate(posts):
    if idx % 100 == 0:
        print(f"Progress: {idx}/{len(posts)} posts ({comments_saved} comments)")
    
    # Get comments for this submission
    url = "https://api.pullpush.io/reddit/search/comment"
    params = {
        'link_id': post_id,  # Just the ID, no t3_ prefix
        'size': 100
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            comments = data.get('data', [])
            
            # Save comments
            for comment in comments:
                try:
                    cur.execute("""
                        INSERT OR IGNORE INTO reddit_comments
                        (id, post_id, subreddit, body, score, created_utc, author)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        comment.get('id'),
                        post_id,
                        comment.get('subreddit'),
                        comment.get('body'),
                        comment.get('score'),
                        comment.get('created_utc'),
                        comment.get('author')
                    ))
                    if cur.rowcount > 0:
                        comments_saved += 1
                except Exception as e:
                    pass  # Ignore duplicates
            
            conn.commit()
            
        elif response.status_code in [500, 525, 522]:
            errors += 1
            if errors > 10:
                print(f"\nToo many errors, pausing 60s...")
                time.sleep(60)
                errors = 0
        
        time.sleep(DELAY)
        
    except KeyboardInterrupt:
        print("\n\nPaused by user")
        break
    except Exception as e:
        errors += 1
        time.sleep(2)

conn.close()

print("\n" + "="*70)
print("FINAL STATS")
print("="*70)
print(f"Posts processed: {len(posts)}")
print(f"Comments saved: {comments_saved}")
print(f"Database: {DB_PATH}")