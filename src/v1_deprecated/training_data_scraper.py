
# training_data_scraper.py - Fetch historical data for model training and backtesting
# Gets data from specified date ranges to validate predictive accuracy

import requests
import csv
import time
from datetime import datetime, timedelta

class TrainingDataScraper:
    """
    Scrapes historical Hacker News data for model training and backtesting
    """
    
    def __init__(self):
        """Initialize the training data scraper"""
        print("Training Data Scraper initialized")
        self.base_url = "https://hn.algolia.com/api/v1/search_by_date"
    
    def get_stories_for_date_range(self, start_date, end_date, stories_per_day=30):
        """
        Fetch stories for a custom date range
        
        Args:
            start_date: datetime object for start date
            end_date: datetime object for end date
            stories_per_day: Number of stories to get per day
            
        Returns:
            List of story dictionaries
        """
        days_total = (end_date - start_date).days + 1
        
        print(f"\nFetching training data:")
        print(f"  Start: {start_date.strftime('%Y-%m-%d')}")
        print(f"  End:   {end_date.strftime('%Y-%m-%d')}")
        print(f"  Days:  {days_total}")
        print(f"  Target: {stories_per_day} stories/day = ~{days_total * stories_per_day} total")
        print("=" * 80)
        
        all_stories = []
        current_date = start_date
        
        while current_date <= end_date:
            # Calculate timestamps for this day
            day_start = int(current_date.timestamp())
            day_end = int((current_date + timedelta(days=1)).timestamp())
            
            # Progress indicator
            days_done = (current_date - start_date).days + 1
            progress = (days_done / days_total) * 100
            
            print(f"[{progress:5.1f}%] {current_date.strftime('%Y-%m-%d')}...", end=" ")
            
            # Fetch stories for this day
            stories = self.fetch_stories_for_day(day_start, day_end, stories_per_day)
            
            print(f"Got {len(stories):3d} stories")
            
            all_stories.extend(stories)
            
            # Move to next day
            current_date += timedelta(days=1)
            
            # Be nice to the API
            time.sleep(1)
        
        print()
        print(f"✅ Total stories collected: {len(all_stories)}")
        
        return all_stories
    
    def fetch_stories_for_day(self, start_timestamp, end_timestamp, max_stories=30):
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
        
        params = {
            'tags': 'story',
            'numericFilters': f'created_at_i>{start_timestamp},created_at_i<{end_timestamp}',
            'hitsPerPage': max_stories
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            
            if response.status_code != 200:
                return stories
            
            data = response.json()
            hits = data.get('hits', [])
            
            for hit in hits:
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
            print(f"Error: {e}")
        
        return stories
    
    def save_training_data(self, stories, filename='data/raw/hackernews_training.csv'):
        """
        Save training data to CSV
        
        Args:
            stories: List of story dictionaries
            filename: Output filename
        """
        if not stories:
            print("No stories to save!")
            return
        
        print(f"\nSaving training data to: {filename}")
        
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
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(stories)
        
        print(f"✅ Saved {len(stories)} stories")
    
    def show_training_statistics(self, stories):
        """
        Display statistics about the training dataset
        
        Args:
            stories: List of story dictionaries
        """
        if not stories:
            return
        
        print("\n" + "=" * 80)
        print("TRAINING DATASET STATISTICS")
        print("=" * 80)
        
        # Date range
        dates = [s['created_at'][:10] for s in stories]
        min_date = min(dates)
        max_date = max(dates)
        
        # Stories by date
        stories_by_date = {}
        for story in stories:
            date = story['created_at'][:10]
            stories_by_date[date] = stories_by_date.get(date, 0) + 1
        
        days_covered = len(stories_by_date)
        avg_per_day = len(stories) / days_covered if days_covered > 0 else 0
        
        print(f"\nTotal stories:      {len(stories)}")
        print(f"Date range:         {min_date} to {max_date}")
        print(f"Days covered:       {days_covered}")
        print(f"Average per day:    {avg_per_day:.1f}")
        print(f"Min stories/day:    {min(stories_by_date.values())}")
        print(f"Max stories/day:    {max(stories_by_date.values())}")
        
        # Top stories
        print("\n📊 Top 10 stories by points:")
        sorted_stories = sorted(stories, key=lambda x: x['points'], reverse=True)
        for i, story in enumerate(sorted_stories[:10], 1):
            print(f"{i:2d}. [{story['points']:4d} pts] {story['title'][:70]}")
            print(f"    {story['created_at'][:10]}")
        
        print("=" * 80)


def get_preset_ranges():
    """
    Get preset date ranges for common training scenarios
    
    Returns:
        Dictionary of preset ranges
    """
    today = datetime.now()
    
    presets = {
        '1': {
            'name': '1 Year (Full historical)',
            'start': today - timedelta(days=365),
            'end': today
        },
        '2': {
            'name': '6 Months (Recent history)',
            'start': today - timedelta(days=180),
            'end': today
        },
        '3': {
            'name': '2024 Full Year',
            'start': datetime(2024, 1, 1),
            'end': datetime(2024, 12, 31)
        },
        '4': {
            'name': '2024 First Half (Jan-June)',
            'start': datetime(2024, 1, 1),
            'end': datetime(2024, 6, 30)
        },
        '5': {
            'name': '2024 Q3-Q4 (July-Dec)',
            'start': datetime(2024, 7, 1),
            'end': datetime(2024, 12, 31)
        },
        '6': {
            'name': 'Custom date range',
            'start': None,  # Will prompt user
            'end': None
        }
    }
    
    return presets


# Main execution
if __name__ == "__main__":
    print("=" * 80)
    print("TRAINING DATA SCRAPER - HISTORICAL DATA FOR MODEL BUILDING")
    print("=" * 80)
    
    # Show preset options
    presets = get_preset_ranges()
    
    print("\nSelect a date range preset:")
    print("-" * 80)
    for key, preset in presets.items():
        print(f"{key}. {preset['name']}")
        if preset['start'] and preset['end']:
            print(f"   ({preset['start'].strftime('%Y-%m-%d')} to {preset['end'].strftime('%Y-%m-%d')})")
    print("-" * 80)
    
    # Get user choice
    choice = input("\nEnter choice (1-6) or press Enter for default (1 year): ").strip()
    
    if not choice:
        choice = '1'
    
    if choice == '6':
        # Custom range
        print("\nEnter custom date range:")
        start_str = input("  Start date (YYYY-MM-DD): ").strip()
        end_str = input("  End date (YYYY-MM-DD): ").strip()
        
        try:
            start_date = datetime.strptime(start_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_str, '%Y-%m-%d')
        except ValueError:
            print("❌ Invalid date format. Using default (1 year).")
            choice = '1'
    
    # Get the date range
    if choice in presets and choice != '6':
        preset = presets[choice]
        start_date = preset['start']
        end_date = preset['end']
        print(f"\n✓ Selected: {preset['name']}")
    
    # Initialize scraper
    scraper = TrainingDataScraper()
    
    # Fetch data
    stories = scraper.get_stories_for_date_range(
        start_date=start_date,
        end_date=end_date,
        stories_per_day=30
    )
    
    # Save and show stats
    if stories:
        scraper.save_training_data(stories)
        scraper.show_training_statistics(stories)
    else:
        print("❌ No stories collected!")
    
    print("\n" + "=" * 80)
    print("Training data collection complete!")
    print("Next steps:")
    print("  1. Process this data: python3 src/text_processor.py (modify for training data)")
    print("  2. Analyze patterns: python3 src/frequency_analyzer.py (modify for training data)")
    print("  3. Backtest predictions against known events")
    print("=" * 80)