"""
Test Pushshift/Pullpush API to see if we can get Reddit data
"""
import requests
import time
from datetime import datetime

# Test endpoint
base_url = "https://api.pullpush.io/reddit/search/submission"

# Get r/technology posts from June 10, 2024
params = {
    'subreddit': 'technology',
    'after': int(datetime(2024, 6, 10).timestamp()),
    'before': int(datetime(2024, 6, 11).timestamp()),
    'size': 100
}

print("Testing Pullpush API...")
print(f"Requesting r/technology posts from June 10, 2024")

try:
    response = requests.get(base_url, params=params, timeout=10)
    
    if response.status_code == 200:
        data = response.json()
        posts = data.get('data', [])
        
        print(f"\n✓ Success!")
        print(f"Retrieved {len(posts)} posts")
        
        if len(posts) > 0:
            print(f"\nSample post:")
            post = posts[0]
            print(f"  Title: {post.get('title', 'N/A')}")
            print(f"  Score: {post.get('score', 'N/A')}")
            print(f"  Comments: {post.get('num_comments', 'N/A')}")
            print(f"  URL: {post.get('url', 'N/A')}")
            
            print(f"\n✓ Free tier works! We can scrape Reddit.")
        else:
            print("\n⚠️  No posts found for this date")
            
    else:
        print(f"\n✗ Error: {response.status_code}")
        print(f"Response: {response.text}")
        
except Exception as e:
    print(f"\n✗ Failed: {e}")

print("\n" + "="*70)
print("RECOMMENDATION")
print("="*70)

print("""
If free tier works:
- Scrape June 8-15 Reddit data (free, takes ~2 hours)
- Validate your HN findings
- No cost

If free tier is blocked:
- Options:
  1. Try different Pushshift mirrors
  2. Use academic torrents (free, slow download)
  3. Skip Reddit, use GitHub issues instead (also free)
  
For your Dec 31 deadline, I recommend sticking with free options.
""")