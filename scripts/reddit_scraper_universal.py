"""
Universal Reddit Scraper
Reads config file for date ranges, subreddits, etc.
Usage: python scripts/reddit_scraper_universal.py --config config/reddit_2024.json
"""
import requests
import time
import sqlite3
import json
import argparse
from datetime import datetime

def log(msg):
    print(msg, flush=True)

def create_database(db_path):
    """Create database schema"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reddit_posts (
            id TEXT PRIMARY KEY,
            subreddit TEXT NOT NULL,
            title TEXT,
            selftext TEXT,
            score INTEGER,
            num_comments INTEGER,
            created_utc INTEGER,
            author TEXT,
            url TEXT,
            comments_scraped BOOLEAN DEFAULT 0
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reddit_comments (
            id TEXT PRIMARY KEY,
            post_id TEXT NOT NULL,
            subreddit TEXT,
            body TEXT,
            score INTEGER,
            created_utc INTEGER,
            author TEXT,
            FOREIGN KEY (post_id) REFERENCES reddit_posts(id)
        )
    """)
    
    cur.execute("CREATE INDEX IF NOT EXISTS idx_posts_scraped ON reddit_posts(comments_scraped)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_comments_post ON reddit_comments(post_id)")
    
    conn.commit()
    conn.close()
    log(f"  ✓ Database initialized: {db_path}")

def collect_posts(config):
    """Collect posts for date range"""
    log("\n" + "="*70)
    log("COLLECTING POSTS")
    log("="*70)
    
    conn = sqlite3.connect(config['database'])
    cur = conn.cursor()
    
    start_ts = int(datetime.strptime(config['start_date'], '%Y-%m-%d').timestamp())
    end_ts = int(datetime.strptime(config['end_date'], '%Y-%m-%d').timestamp())
    
    total_posts = 0
    
    for subreddit in config['subreddits']:
        log(f"\n📊 Subreddit: r/{subreddit}")
        
        before_ts = end_ts
        subreddit_posts = 0
        
        while True:
            try:
                response = requests.get(
                    "https://api.pullpush.io/reddit/search/submission",
                    params={
                        'subreddit': subreddit,
                        'after': start_ts,
                        'before': before_ts,
                        'size': 100,
                        'sort': 'desc'
                    },
                    timeout=30
                )
                
                if response.status_code != 200:
                    log(f"  ✗ HTTP {response.status_code}")
                    break
                
                posts = response.json().get('data', [])
                
                if len(posts) == 0:
                    log(f"  ✓ No more posts (total: {subreddit_posts})")
                    break
                
                # Insert posts
                for post in posts:
                    cur.execute("""
                        INSERT OR IGNORE INTO reddit_posts
                        (id, subreddit, title, selftext, score, num_comments, 
                         created_utc, author, url, comments_scraped)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """, (
                        post.get('id'), post.get('subreddit'),
                        post.get('title'), post.get('selftext'),
                        post.get('score'), post.get('num_comments'),
                        post.get('created_utc'), post.get('author'),
                        post.get('url')
                    ))
                
                conn.commit()
                subreddit_posts += len(posts)
                total_posts += len(posts)
                
                # Update pagination
                before_ts = min(p.get('created_utc', before_ts) for p in posts)
                
                log(f"  Fetched {len(posts)} posts (subreddit total: {subreddit_posts})")
                time.sleep(config['rate_limit_delay'])
                
            except Exception as e:
                log(f"  ✗ Error: {e}")
                time.sleep(5)
    
    conn.close()
    log(f"\n✓ POST COLLECTION COMPLETE: {total_posts:,} posts")
    return total_posts

def collect_comments(config):
    """Collect comments for posts"""
    log("\n" + "="*70)
    log("COLLECTING COMMENTS")
    log("="*70)
    
    conn = sqlite3.connect(config['database'])
    cur = conn.cursor()
    
    # Get posts needing comments
    cur.execute("SELECT COUNT(*) FROM reddit_posts WHERE comments_scraped = 0")
    total_pending = cur.fetchone()[0]
    log(f"\nPosts needing comments: {total_pending:,}")
    
    chunk_size = config['chunk_size']
    offset = 0
    total_comments = 0
    session_posts = 0
    
    try:
        while True:
            # Get chunk
            cur.execute("""
                SELECT id FROM reddit_posts 
                WHERE comments_scraped = 0 
                LIMIT ? OFFSET ?
            """, (chunk_size, offset))
            
            batch = cur.fetchall()
            
            if not batch:
                log("\n✓ ALL COMMENTS COLLECTED")
                break
            
            log(f"\nProcessing chunk {offset}-{offset+len(batch)}...")
            
            for (post_id,) in batch:
                try:
                    response = requests.get(
                        "https://api.pullpush.io/reddit/search/comment",
                        params={
                            'link_id': post_id,
                            'size': config['comments_per_post']
                        },
                        timeout=20
                    )
                    
                    if response.status_code == 200:
                        comments = response.json().get('data', [])
                        
                        for c in comments:
                            cur.execute("""
                                INSERT OR IGNORE INTO reddit_comments
                                (id, post_id, subreddit, body, score, created_utc, author)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, (
                                c.get('id'), post_id, c.get('subreddit'),
                                c.get('body'), c.get('score'),
                                c.get('created_utc'), c.get('author')
                            ))
                            
                            if cur.rowcount > 0:
                                total_comments += 1
                        
                        cur.execute(
                            "UPDATE reddit_posts SET comments_scraped = 1 WHERE id = ?",
                            (post_id,)
                        )
                        session_posts += 1
                    
                    time.sleep(config['rate_limit_delay'])
                    
                except Exception as e:
                    log(f"  ✗ Error on {post_id}: {e}")
                    time.sleep(2)
            
            conn.commit()
            log(f"  ✓ Chunk done. Session: +{session_posts} posts, +{total_comments:,} comments")
            offset += chunk_size
            
    except KeyboardInterrupt:
        log("\n\nPaused by user")
        conn.commit()
    
    conn.close()
    log(f"\n✓ COMMENT COLLECTION COMPLETE: {total_comments:,} comments")
    return total_comments

def main():
    parser = argparse.ArgumentParser(description='Universal Reddit Scraper')
    parser.add_argument('--config', required=True, help='Path to config JSON file')
    parser.add_argument('--posts-only', action='store_true', help='Only collect posts')
    parser.add_argument('--comments-only', action='store_true', help='Only collect comments')
    
    args = parser.parse_args()
    
    # Load config
    with open(args.config, 'r') as f:
        config = json.load(f)
    
    log("="*70)
    log("REDDIT SCRAPER UNIVERSAL")
    log("="*70)
    log(f"\nConfig: {args.config}")
    log(f"Year: {config['year']}")
    log(f"Date range: {config['start_date']} to {config['end_date']}")
    log(f"Database: {config['database']}")
    log(f"Subreddits: {len(config['subreddits'])}")
    
    # Create database
    create_database(config['database'])
    
    # Collect data
    if args.comments_only:
        collect_comments(config)
    elif args.posts_only:
        collect_posts(config)
    else:
        collect_posts(config)
        collect_comments(config)
    
    log("\n" + "="*70)
    log("SCRAPING COMPLETE")
    log("="*70)

if __name__ == '__main__':
    main()