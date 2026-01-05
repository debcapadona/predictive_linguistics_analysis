"""
rss_feeds.py - RSS feed collector

Collects articles from multiple RSS feeds
Config-driven for any RSS source
"""

import feedparser
import csv
import time
from datetime import datetime
import yaml
import os
from typing import List, Dict
import sys


class RSSFeedCollector:
    """Collect data from RSS feeds"""
    
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
        print("✓ RSS Feed Collector initialized")
    
    def validate_config(self):
        """Validate configuration"""
        required = ['feeds', 'output', 'collection']
        for field in required:
            if field not in self.config:
                raise ValueError(f"Missing required config section: {field}")
        
        if not self.config['feeds']:
            raise ValueError("No RSS feeds specified in config")
    
    def parse_date(self, date_str: str) -> datetime:
        """
        Parse various RSS date formats
        
        Args:
            date_str: Date string from feed
            
        Returns:
            datetime object
        """
        if not date_str:
            return datetime.now()
        
        # feedparser handles most formats automatically
        try:
            parsed = feedparser._parse_date(date_str)
            if parsed:
                return datetime(*parsed[:6])
        except:
            pass
        
        return datetime.now()
    
    def fetch_feed(self, feed_url: str, feed_name: str) -> List[Dict]:
        """
        Fetch entries from a single RSS feed
        
        Args:
            feed_url: URL of RSS feed
            feed_name: Name of feed (for tracking)
            
        Returns:
            List of article dictionaries
        """
        print(f"  Fetching {feed_name}...")
        
        try:
            feed = feedparser.parse(feed_url)
            
            articles = []
            for entry in feed.entries:
                article = {
                    'source': feed_name,
                    'title': entry.get('title', ''),
                    'url': entry.get('link', ''),
                    'published': entry.get('published', ''),
                    'summary': entry.get('summary', ''),
                    'created_at': self.parse_date(entry.get('published', '')).strftime('%Y-%m-%d %H:%M:%S')
                }
                articles.append(article)
            
            print(f"    Got {len(articles)} articles")
            return articles
            
        except Exception as e:
            print(f"    Error fetching {feed_name}: {e}")
            return []
    
    def collect_data(self):
        """
        Collect data from all configured feeds
        
        Returns:
            List of all collected articles
        """
        print("\n" + "=" * 120)
        print("COLLECTING RSS FEED DATA")
        print("=" * 120)
        
        feeds = self.config['feeds']
        delay = self.config['collection'].get('delay_seconds', 2)
        
        print(f"Feeds to collect: {len(feeds)}")
        print("=" * 120)
        
        all_articles = []
        
        for feed in feeds:
            feed_name = feed['name']
            feed_url = feed['url']
            
            articles = self.fetch_feed(feed_url, feed_name)
            all_articles.extend(articles)
            
            time.sleep(delay)
        
        print(f"\n✅ Total articles collected: {len(all_articles)}")
        return all_articles
    
    def filter_by_date_range(self, articles: List[Dict]) -> List[Dict]:
        """
        Filter articles by date range if specified
        
        Args:
            articles: List of articles
            
        Returns:
            Filtered articles
        """
        date_range = self.config['collection'].get('date_range')
        if not date_range:
            return articles
        
        start_date = datetime.strptime(date_range['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(date_range['end_date'], '%Y-%m-%d')
        
        filtered = []
        for article in articles:
            try:
                article_date = datetime.strptime(article['created_at'], '%Y-%m-%d %H:%M:%S')
                if start_date <= article_date < end_date:
                    filtered.append(article)
            except:
                continue
        
        print(f"Filtered to {len(filtered)} articles within date range")
        return filtered
    
    def save_data(self, articles: List[Dict]):
        """
        Save collected articles to CSV
        
        Args:
            articles: List of article dictionaries
        """
        output_config = self.config['output']
        output_file = output_config['file']
        
        # Create directory if needed
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        print(f"\nSaving data to: {output_file}")
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            if articles:
                fieldnames = ['source', 'title', 'url', 'published', 'summary', 'created_at']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(articles)
        
        print(f"✅ Saved {len(articles)} articles")
    
    def run(self):
        """Run the complete collection process"""
        print("=" * 120)
        print("RSS FEED COLLECTOR")
        print("=" * 120)
        
        articles = self.collect_data()
        
        # Filter by date range if specified
        articles = self.filter_by_date_range(articles)
        
        self.save_data(articles)
        
        print("\n" + "=" * 120)
        print("COLLECTION COMPLETE")
        print("=" * 120)
        print(f"Source file: {self.config['output']['file']}")
        print(f"Total articles: {len(articles)}")
        
        if articles:
            # Show sources breakdown
            sources = {}
            for article in articles:
                source = article['source']
                sources[source] = sources.get(source, 0) + 1
            
            print("\n📊 Articles by source:")
            for source, count in sorted(sources.items(), key=lambda x: x[1], reverse=True):
                print(f"  {source}: {count}")
        
        print("\nNEXT: Process this data with text_processor")
        print("=" * 120)


# Main execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python3 -m src.collectors.rss_feeds <config_file.yaml>")
        print("\nExample: python3 -m src.collectors.rss_feeds configs/sources/rss_feeds.yaml")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    collector = RSSFeedCollector(config_path)
    collector.run()