"""
hackernews.py - Hacker News data collector

Collects historical data from Hacker News Algolia API
Config-driven for reusability
"""

import requests
import csv
import time
from datetime import datetime, timedelta
import yaml
import os
from typing import List, Dict

class HackerNewsCollector:
    """Collect data from Hacker News"""
    
    def __init__(self, config_path: str):
        """
        Initialize collector with configuration
        
        Args:
            config_path: Path to YAML configuration file
        """
        print(f"Loading configuration from: {config_path}")
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.validate_config()
        print("✓ Configuration loaded and validated")
    
    def validate_config(self):
        """Validate configuration"""
        required = ['date_range', 'output', 'api']
        for field in required:
            if field not in self.config:
                raise ValueError(f"Missing required config section: {field}")
        
        # Validate dates
        dr = self.config['date_range']
        for date_field in ['start_date', 'end_date']:
            if date_field not in dr:
                raise ValueError(f"Missing required date: {date_field}")
    
    def fetch_stories_for_date(self, date: datetime) -> List[Dict]:
        """
        Fetch top stories for a specific date
        
        Args:
            date: Date to fetch stories for
            
        Returns:
            List of story dictionaries
        """
        api_config = self.config['api']
        base_url = api_config.get('base_url', 'https://hn.algolia.com/api/v1/search_by_date')
        stories_per_day = api_config.get('stories_per_day', 30)
        
        # Calculate timestamp range for the day
        start_ts = int(date.timestamp())
        end_ts = int((date + timedelta(days=1)).timestamp())
        
        params = {
            'tags': 'story',
            'numericFilters': f'created_at_i>{start_ts},created_at_i<{end_ts}',
            'hitsPerPage': stories_per_day
        }
        
        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            stories = []
            for hit in data.get('hits', []):
                story = {
                    'id': hit.get('objectID'),
                    'title': hit.get('title', ''),
                    'url': hit.get('url', ''),
                    'points': hit.get('points', 0),
                    'num_comments': hit.get('num_comments', 0),
                    'created_at': datetime.fromtimestamp(hit.get('created_at_i', 0)).strftime('%Y-%m-%d %H:%M:%S')
                }
                stories.append(story)
            
            return stories
            
        except Exception as e:
            print(f"  Error fetching {date.strftime('%Y-%m-%d')}: {e}")
            return []
    
    def collect_data(self):
        """
        Collect data for configured date range
        
        Returns:
            List of all collected stories
        """
        dr = self.config['date_range']
        start_date = datetime.strptime(dr['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(dr['end_date'], '%Y-%m-%d')
        
        api_config = self.config['api']
        delay = api_config.get('delay_seconds', 1)
        
        print("\n" + "=" * 120)
        print("COLLECTING HACKER NEWS DATA")
        print("=" * 120)
        print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"Days: {(end_date - start_date).days}")
        print(f"Stories per day: {api_config.get('stories_per_day', 30)}")
        print("=" * 120)
        
        all_stories = []
        current_date = start_date
        total_days = (end_date - start_date).days
        day_count = 0
        
        while current_date < end_date:
            day_count += 1
            progress = (day_count / total_days) * 100
            
            print(f"[{progress:5.1f}%] {current_date.strftime('%Y-%m-%d')}...", end='', flush=True)
            
            stories = self.fetch_stories_for_date(current_date)
            all_stories.extend(stories)
            
            print(f" Got {len(stories):3d} stories")
            
            current_date += timedelta(days=1)
            time.sleep(delay)
        
        print(f"\n✅ Total stories collected: {len(all_stories)}")
        return all_stories
    
    def save_data(self, stories: List[Dict]):
        """
        Save collected stories to CSV
        
        Args:
            stories: List of story dictionaries
        """
        output_config = self.config['output']
        output_file = output_config['file']
        
        # Create directory if needed
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        print(f"\nSaving data to: {output_file}")
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            if stories:
                fieldnames = stories[0].keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(stories)
        
        print(f"✅ Saved {len(stories)} stories")
    
    def run(self):
        """Run the complete collection process"""
        print("=" * 120)
        print("HACKER NEWS COLLECTOR")
        print("=" * 120)
        
        stories = self.collect_data()
        self.save_data(stories)
        
        print("\n" + "=" * 120)
        print("COLLECTION COMPLETE")
        print("=" * 120)
        print(f"Source file: {self.config['output']['file']}")
        print(f"Total stories: {len(stories)}")
        
        if stories:
            # Show top stories
            sorted_stories = sorted(stories, key=lambda x: x['points'], reverse=True)
            print("\n📊 Top 5 stories by points:")
            for i, story in enumerate(sorted_stories[:5], 1):
                print(f" {i}. [{story['points']} pts] {story['title'][:70]}")
                print(f"    {story['created_at']}")
        
        print("\nNEXT: Process this data with text_processor")
        print("=" * 120)


# Main execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python3 -m src.collectors.hackernews <config_file.yaml>")
        print("\nExample: python3 -m src.collectors.hackernews configs/sources/hackernews.yaml")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    collector = HackerNewsCollector(config_path)
    collector.run()
