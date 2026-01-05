"""
data_processor.py - Process raw data into analyzed format

Reads raw CSV data, applies text processing, saves processed output
Config-driven for any data source
"""

import csv
import os
import yaml
from datetime import datetime
from typing import List, Dict
import sys

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.text_processor import TextProcessor


class DataProcessor:
    """Process raw data files"""
    
    def __init__(self, config_path: str):
        """
        Initialize processor with configuration
        
        Args:
            config_path: Path to YAML configuration file
        """
        print(f"Loading configuration from: {config_path}")
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize text processor with config parameters
        processing_config = self.config.get('processing', {})
        self.text_processor = TextProcessor(
            min_word_length=processing_config.get('min_word_length', 3),
            remove_stopwords=processing_config.get('remove_stopwords', True)
        )
        
        print("✓ Data Processor initialized")
    
    def load_raw_data(self, input_file: str) -> List[Dict]:
        """
        Load raw data from CSV
        
        Args:
            input_file: Path to raw CSV file
            
        Returns:
            List of raw data dictionaries
        """
        print(f"Loading raw data from: {input_file}")
        
        items = []
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                items.append(row)
        
        print(f"✓ Loaded {len(items)} items")
        return items
    
    def process_item(self, item: Dict) -> Dict:
        """
        Process a single item
        
        Args:
            item: Raw item dictionary
            
        Returns:
            Processed item dictionary
        """
        # Get text field (configurable)
        text_field = self.config['processing'].get('text_field', 'title')
        text = item.get(text_field, '')
        
        # Process text
        processed = self.text_processor.process_text(text)
        
        # Build output record
        output = {
            'id': item.get('id', ''),
            'story_id': item.get('story_id', ''),
            'parent_id': item.get('parent_id', ''),
            'original_title': processed['original'],
            'cleaned_title': processed['cleaned'],
            'created_at': item.get('created_at', ''),
            'points': item.get('points', 0),
            'num_comments': item.get('num_comments', 0),
            'words': '|'.join(processed['tokens']),
            'bigrams': '|'.join(processed['bigrams']),
            'trigrams': '|'.join(processed['trigrams']),
            'word_count': processed['token_count']
        }
        
        return output
    
    def process_data(self, items: List[Dict]) -> List[Dict]:
        """
        Process all items
        
        Args:
            items: List of raw items
            
        Returns:
            List of processed items
        """
        print("\nProcessing items...")
        processed_items = []
        
        total = len(items)
        for i, item in enumerate(items, 1):
            if i % 100 == 0:
                progress = (i / total) * 100
                print(f"  Progress: {progress:.1f}% ({i}/{total})")
            
            processed = self.process_item(item)
            processed_items.append(processed)
        
        print(f"✓ Processed {len(processed_items)} items")
        return processed_items
    
    def save_processed_data(self, items: List[Dict], output_file: str):
        """
        Save processed data to CSV
        
        Args:
            items: List of processed items
            output_file: Path to output CSV file
        """
        print(f"\nSaving processed data to: {output_file}")
        
        # Create directory if needed
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Save to CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            if items:
                fieldnames = items[0].keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(items)
        
        print(f"✓ Saved {len(items)} processed items")
    
    def run(self):
        """Run the complete processing pipeline"""
        print("=" * 120)
        print("DATA PROCESSOR")
        print("=" * 120)
        
        # Get file paths from config
        input_file = self.config['files']['input']
        output_file = self.config['files']['output']
        
        # Load raw data
        raw_items = self.load_raw_data(input_file)
        
        # Process
        processed_items = self.process_data(raw_items)
        
        # Save
        self.save_processed_data(processed_items, output_file)
        
        print("\n" + "=" * 120)
        print("PROCESSING COMPLETE")
        print("=" * 120)
        print(f"Input:  {input_file}")
        print(f"Output: {output_file}")
        print(f"Items:  {len(processed_items)}")
        
        if processed_items:
            # Show sample
            sample = processed_items[0]
            print("\nSample processed item:")
            print(f"  Title: {sample['original_title'][:70]}")
            print(f"  Words: {len(sample['words'].split('|'))} tokens")
            print(f"  Bigrams: {len(sample['bigrams'].split('|')) if sample['bigrams'] else 0}")
        
        print("\nNEXT: Run analyzers on processed data!")
        print("=" * 120)


# Main execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python3 -m src.core.data_processor <config_file.yaml>")
        print("\nExample: python3 -m src.core.data_processor configs/processing/hackernews_test.yaml")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    processor = DataProcessor(config_path)
    processor.run()