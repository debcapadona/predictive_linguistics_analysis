"""
Fixed Reddit comment scraper - ALL 254,700 posts from 2024
Resumes from where it left off
"""
import requests
import time
import sqlite3
from datetime import datetime

print("="*70)
print("REDDIT COMMENT SCRAPER (FULL 2024)")
print("="*70)

DB_PATH = 'data/reddit_data.db'
DELAY = 0.6  # seconds between requests (100/min)
BATCH_SIZE = 1000  # Commit every 1000 posts

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Add column to track which posts have comments scraped
try:
    cur.execute("ALTER TABLE reddit_posts ADD COLUMN comments_scraped BOOLEAN DEFAULT 0")
    conn.commit()
    print("✓ Added comments_scraped column")
except:
    print("✓ Column already exists")

# Get posts that need comment scraping
print("\nCounting posts without comments...")
cur.execute("""
    SELECT COUNT(*) 
    FROM reddit_posts 
    WHERE comments_scraped = 0 OR comments_scraped IS NULL
""")
total_remaining = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM reddit_posts")
total_posts = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM reddit_comments")
existing_comments = cur.fetchone()[0]

print(f"Total posts: {total_posts:,}")
print(f"Already scraped: {total_posts - total_remaining:,}")
print(f"Remaining: {total_remaining:,}")
print(f"Existing comments: {existing_comments:,}")

# Estimate time
hours = (total_remaining * DELAY) / 3600
print(f"\nEstimated time: {hours:.1f} hours")
print("Press Ctrl+C to pause (progress saves every 1000 posts)")
print("="*70)

comments_saved = 0
posts_processed = 0
errors = 0
start_time = time.time()

try:
    while True:
        # Get batch of posts without comments
        cur.execute("""
            SELECT id, subreddit
            FROM reddit_posts 
            WHERE comments_scraped = 0 OR comments_scraped IS NULL
            LIMIT ?
        """, (BATCH_SIZE,))
        
        batch = cur.fetchall()
        
        if not batch:
            print("\n✓ All posts processed!")
            break
        
        for post_id, subreddit in batch:
            # Get comments for this post
            url = "https://api.pullpush.io/reddit/search/comment"
            params = {
                'link_id': post_id,  # Just ID, no prefix
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
                        except:
                            pass
                    
                    # Mark post as scraped
                    cur.execute("""
                        UPDATE reddit_posts 
                        SET comments_scraped = 1 
                        WHERE id = ?
                    """, (post_id,))
                    
                    posts_processed += 1
                    
                    # Progress update
                    if posts_processed % 100 == 0:
                        elapsed = time.time() - start_time
                        rate = posts_processed / elapsed if elapsed > 0 else 0
                        remaining_posts = total_remaining - posts_processed
                        eta_seconds = remaining_posts / rate if rate > 0 else 0
                        eta_hours = eta_seconds / 3600
                        
                        print(f"Progress: {posts_processed:,}/{total_remaining:,} posts "
                              f"({comments_saved:,} comments) "
                              f"| Rate: {rate:.1f} posts/sec "
                              f"| ETA: {eta_hours:.1f}h", flush=True)
                    
                elif response.status_code in [500, 525, 522]:
                    errors += 1
                    if errors > 20:
                        print(f"  ⚠️  Too many errors, pausing 60s...")
                        time.sleep(60)
                        errors = 0
                
                time.sleep(DELAY)
                
            except Exception as e:
                errors += 1
                if errors % 10 == 0:
                    print(f"  ⚠️  Error count: {errors}")
                time.sleep(2)
        
        # Commit batch
        conn.commit()
        print(f"  ✓ Committed batch ({len(batch)} posts)")

except KeyboardInterrupt:
    print("\n\n⏸️  Paused by user")
    conn.commit()
    print("  ✓ Progress saved")

conn.close()

# Final stats
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM reddit_posts WHERE comments_scraped = 1")
scraped = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM reddit_comments")
total_comments = cur.fetchone()[0]

conn.close()

print("\n" + "="*70)
print("FINAL STATS")
print("="*70)
print(f"Posts scraped: {scraped:,}/{total_posts:,}")
print(f"Total comments: {total_comments:,}")
print(f"Comments per post: {total_comments/scraped:.1f}" if scraped > 0 else "N/A")
print(f"Database: {DB_PATH}")