"""
hackernews_comments.py - Hacker News comment collector

Collects comment threads from HN stories
Uses Firebase API to get full comment trees
"""

import requests
import csv
import time
from datetime import datetime
import yaml
import os
from typing import List, Dict, Optional
import sys


class HackerNewsCommentCollector:
    """Collect comments from Hacker News stories"""
    
    def __init__(self, config_path: str):
        """
        Initialize collector with configuration
        
        Args:
            config_path: Path to YAML configuration file
        """
        print(f"Loading configuration from: {config_path}")
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.base_url = "https://hacker-news.firebaseio.com/v0"
        self.validate_config()
        print("✓ HN Comment Collector initialized")
    
    def validate_config(self):
        """Validate configuration"""
        required = ['input', 'output', 'collection']
        for field in required:
            if field not in self.config:
                raise ValueError(f"Missing required config section: {field}")
    
    def fetch_item(self, item_id: int) -> Optional[Dict]:
        """
        Fetch a single item (story or comment) from HN API
        
        Args:
            item_id: HN item ID
            
        Returns:
            Item dictionary or None
        """
        try:
            url = f"{self.base_url}/item/{item_id}.json"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return None
    
    def fetch_comments_recursive(self, item_id: int, depth: int = 0, 
                                 max_depth: int = 5) -> List[Dict]:
        """
        Recursively fetch all comments in a thread
        
        Args:
            item_id: Comment or story ID
            depth: Current recursion depth
            max_depth: Maximum depth to traverse
            
        Returns:
            List of comment dictionaries
        """
        if depth > max_depth:
            return []
        
        item = self.fetch_item(item_id)
        if not item:
            return []
        
        comments = []
        
        # If this is a comment, add it
        if item.get('type') == 'comment':
            comment = {
                'id': item.get('id'),
                'parent_id': item.get('parent'),
                'author': item.get('by', 'unknown'),
                'text': item.get('text', ''),
                'created_at': datetime.fromtimestamp(item.get('time', 0)).strftime('%Y-%m-%d %H:%M:%S'),
                'depth': depth
            }
            comments.append(comment)
        
        # Recursively fetch child comments
        if 'kids' in item:
            for kid_id in item['kids']:
                child_comments = self.fetch_comments_recursive(
                    kid_id, depth + 1, max_depth
                )
                comments.extend(child_comments)
                
                # Small delay to be polite
                time.sleep(0.1)
        
        return comments
    
    def load_stories(self, input_file: str) -> List[Dict]:
        """
        Load stories from existing HN data CSV
        
        Args:
            input_file: Path to CSV with HN stories
            
        Returns:
            List of story dictionaries
        """
        print(f"Loading stories from: {input_file}")
        
        stories = []
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                stories.append(row)
        
        print(f"Loaded {len(stories)} stories")
        return stories
    
    def collect_comments(self, stories: List[Dict]):
        """
        Collect comments for all stories
        
        Args:
            stories: List of story dictionaries
            
        Returns:
            List of all comments
        """
        collection_config = self.config['collection']
        max_stories = collection_config.get('max_stories', None)
        max_depth = collection_config.get('max_comment_depth', 3)
        min_comments = collection_config.get('min_comments', 5)
        
        # Filter stories by comment count
        stories_with_comments = [
            s for s in stories
            if int(s.get('num_comments', 0)) >= min_comments
        ]
        
        if max_stories:
            stories_with_comments = stories_with_comments[:max_stories]
        
        print("\n" + "=" * 120)
        print("COLLECTING COMMENTS")
        print("=" * 120)
        print(f"Stories to process: {len(stories_with_comments)}")
        print(f"Max comment depth: {max_depth}")
        print(f"Min comments per story: {min_comments}")
        print("=" * 120)
        
        all_comments = []
        
        for i, story in enumerate(stories_with_comments, 1):
            story_id = story['id']
            num_comments = int(story.get('num_comments', 0))
            
            progress = (i / len(stories_with_comments)) * 100
            print(f"\n[{progress:5.1f}%] Story {i}/{len(stories_with_comments)}: ID={story_id} ({num_comments} comments)")
            print(f"  Title: {story.get('title', '')[:70]}")
            
            # Fetch story item to get comment IDs
            story_item = self.fetch_item(int(story_id))
            
            if not story_item or 'kids' not in story_item:
                print(f"  ⚠️  No comments found")
                continue
            
            # Fetch all comments recursively
            story_comments = []
            for kid_id in story_item['kids']:
                thread_comments = self.fetch_comments_recursive(
                    kid_id, depth=0, max_depth=max_depth
                )
                story_comments.extend(thread_comments)
                time.sleep(0.2)  # Be polite to API
            
            # Add story ID to each comment
            for comment in story_comments:
                comment['story_id'] = story_id
                comment['story_title'] = story.get('title', '')
            
            all_comments.extend(story_comments)
            print(f"  ✓ Collected {len(story_comments)} comments")
        
        print(f"\n✅ Total comments collected: {len(all_comments)}")
        return all_comments
    
    def save_comments(self, comments: List[Dict]):
        """
        Save collected comments to CSV
        
        Args:
            comments: List of comment dictionaries
        """
        output_config = self.config['output']
        output_file = output_config['file']
        
        # Create directory if needed
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        print(f"\nSaving comments to: {output_file}")
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            if comments:
                fieldnames = ['id', 'story_id', 'story_title', 'parent_id', 
                             'author', 'text', 'created_at', 'depth']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(comments)
        
        print(f"✅ Saved {len(comments)} comments")
    
    def run(self):
        """Run the complete comment collection process"""
        print("=" * 120)
        print("HACKER NEWS COMMENT COLLECTOR")
        print("=" * 120)
        
        # Load stories
        input_file = self.config['input']['file']
        stories = self.load_stories(input_file)
        
        # Collect comments
        comments = self.collect_comments(stories)
        
        # Save comments
        self.save_comments(comments)
        
        print("\n" + "=" * 120)
        print("COLLECTION COMPLETE")
        print("=" * 120)
        print(f"Input:  {input_file}")
        print(f"Output: {self.config['output']['file']}")
        print(f"Comments: {len(comments)}")
        
        if comments:
            # Show author breakdown
            authors = {}
            for comment in comments:
                author = comment['author']
                authors[author] = authors.get(author, 0) + 1
            
            print(f"\nTop commenters:")
            top_authors = sorted(authors.items(), key=lambda x: x[1], reverse=True)[:5]
            for author, count in top_authors:
                print(f"  {author}: {count} comments")
        
        print("\nNEXT: Process comments with text_processor")
        print("=" * 120)


# Main execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python3 -m src.collectors.hackernews_comments <config_file.yaml>")
        print("\nExample: python3 -m src.collectors.hackernews_comments configs/sources/hn_comments.yaml")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    collector = HackerNewsCommentCollector(config_path)
    collector.run()