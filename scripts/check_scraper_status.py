import sqlite3
from datetime import datetime

conn = sqlite3.connect('data/reddit_data.db')
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM reddit_posts WHERE comments_scraped = 1")
done = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM reddit_posts")
total = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM reddit_comments")
comments = cur.fetchone()[0]

remaining = total - done
percent = (done / total * 100) if total > 0 else 0

timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

print(f"Timestamp: {timestamp}")
print(f"Posts: {done:,}/{total:,} ({percent:.1f}%)")
print(f"Remaining: {remaining:,}")
print(f"Comments: {comments:,}")
print(f"\n# Copy this for comparison:")
print(f"{timestamp}|{done}|{total}|{comments}")

conn.close()