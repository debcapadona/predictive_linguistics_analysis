# historical_scraper.py - Fetch historical Hacker News data
# This uses the HN Algolia API to get stories from the past 30 days

import requests
import csv
import time
from datetime import datetime, timedelta

def get_stories_for_date_range(days_back=30, stories_per_day=30):
    """
    Fetches stories from Hacker News for the specified date range
    
    Args:
        days_back: How many days back to scrape (default: 30)
        stories_per_day: Number of stories to get per day (default: 30)
    
    Returns:
        List of story dictionaries
    """
    print(f"Fetching stories from the last {days_back} days...")
    print(f"Target: {stories_per_day} stories per day")
    print("=" * 60)
    
    all_stories = []
    
    # Calculate timestamp for X days ago
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    # Convert to Unix timestamp (seconds since 1970)
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())
    
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"Timestamp range: {start_timestamp} to {end_timestamp}")
    print()
    
    # Process one day at a time
    current_date = start_date
    
    while current_date <= end_date:
        # Calculate timestamps for this day
        day_start = int(current_date.timestamp())
        day_end = int((current_date + timedelta(days=1)).timestamp())
        
        print(f"Fetching: {current_date.strftime('%Y-%m-%d')}...", end=" ")
        
        # Fetch stories for this day
        stories = fetch_stories_for_day(day_start, day_end, stories_per_day)
        
        print(f"Got {len(stories)} stories")
        
        all_stories.extend(stories)
        
        # Move to next day
        current_date += timedelta(days=1)
        
        # Be nice to the API - wait a bit between requests
        time.sleep(1)
    
    print()
    print(f"Total stories collected: {len(all_stories)}")
    
    return all_stories

def fetch_stories_for_day(start_timestamp, end_timestamp, max_stories=30):
    """
    Fetch stories for a specific day using HN Algolia API
    
    Args:
        start_timestamp: Unix timestamp for start of day
        end_timestamp: Unix timestamp for end of day
        max_stories: Maximum number of stories to fetch
    
    Returns:
        List of story dictionaries
    """
    stories = []
    
    # Algolia API endpoint
    base_url = "https://hn.algolia.com/api/v1/search_by_date"
    
    # Build query parameters
    params = {
        'tags': 'story',
        'numericFilters': f'created_at_i>{start_timestamp},created_at_i<{end_timestamp}',
        'hitsPerPage': max_stories
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        
        if response.status_code != 200:
            print(f"Error: Status {response.status_code}")
            return stories
        
        data = response.json()
        hits = data.get('hits', [])
        
        for hit in hits:
            # Extract story data
            story = {
                'id': hit.get('objectID', ''),
                'title': hit.get('title', ''),
                'url': hit.get('url', ''),
                'points': hit.get('points', 0),
                'author': hit.get('author', ''),
                'num_comments': hit.get('num_comments', 0),
                'created_at': datetime.fromtimestamp(
                    hit.get('created_at_i', 0)
                ).strftime('%Y-%m-%d %H:%M:%S'),
                'created_at_timestamp': hit.get('created_at_i', 0)
            }
            
            stories.append(story)
    
    except Exception as e:
        print(f"Error fetching data: {e}")
    
    return stories

def save_to_csv(stories, filename='data/raw/hackernews_historical.csv'):
    """
    Save stories to CSV file
    
    Args:
        stories: List of story dictionaries
        filename: Output CSV filename
    """
    if not stories:
        print("No stories to save!")
        return
    
    print(f"\nSaving {len(stories)} stories to {filename}...")
    
    # Define CSV columns
    fieldnames = [
        'id', 
        'title', 
        'url', 
        'points', 
        'author', 
        'num_comments', 
        'created_at',
        'created_at_timestamp'
    ]
    
    # Write to CSV
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(stories)
    
    print(f"✅ Saved successfully!")

def show_statistics(stories):
    """
    Display statistics about collected stories
    
    Args:
        stories: List of story dictionaries
    """
    if not stories:
        return
    
    print("\n" + "=" * 60)
    print("STATISTICS")
    print("=" * 60)
    
    # Count stories by date
    stories_by_date = {}
    for story in stories:
        date = story['created_at'][:10]  # Get just the date part
        stories_by_date[date] = stories_by_date.get(date, 0) + 1
    
    print(f"\nTotal stories: {len(stories)}")
    print(f"Date range: {min(stories_by_date.keys())} to {max(stories_by_date.keys())}")
    print(f"Days covered: {len(stories_by_date)}")
    print(f"Average per day: {len(stories) / len(stories_by_date):.1f}")
    
    # Top 5 stories by points
    print("\nTop 5 stories by points:")
    sorted_stories = sorted(stories, key=lambda x: x['points'], reverse=True)
    for i, story in enumerate(sorted_stories[:5], 1):
        print(f"{i}. {story['title'][:60]}...")
        print(f"   Points: {story['points']} | Comments: {story['num_comments']} | {story['created_at']}")

# Main execution
if __name__ == "__main__":
    print("=" * 60)
    print("HACKER NEWS HISTORICAL SCRAPER")
    print("=" * 60)
    print()
    
    # Configuration
    DAYS_BACK = 30  # How many days of history to fetch
    STORIES_PER_DAY = 30  # Stories per day
    
    # Fetch historical data
    stories = get_stories_for_date_range(
        days_back=DAYS_BACK,
        stories_per_day=STORIES_PER_DAY
    )
    
    # Save to CSV
    if stories:
        save_to_csv(stories)
        show_statistics(stories)
    else:
        print("❌ No stories collected!")
    
    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)