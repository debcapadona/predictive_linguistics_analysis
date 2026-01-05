# view_data.py - Quick data viewer with nice formatting

import csv
import sys

def view_csv(filename, num_rows=20):
    """Display CSV in a readable format"""
    
    print(f"\nViewing: {filename}")
    print("=" * 120)
    
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        # Get column names
        fieldnames = reader.fieldnames
        print("\nColumns:", ', '.join(fieldnames))
        
        # Count total rows
        rows = list(reader)
        print(f"Total rows: {len(rows)}")
        
        # Show first N rows
        print(f"\nFirst {num_rows} rows:")
        print("-" * 120)
        
        for i, row in enumerate(rows[:num_rows], 1):
            print(f"\n{i}. ID: {row.get('id', 'N/A')}")
            print(f"   Title: {row.get('title', 'N/A')[:80]}")
            print(f"   Date: {row.get('created_at', 'N/A')}")
            print(f"   Points: {row.get('points', 0)} | Comments: {row.get('num_comments', 0)}")
            if row.get('url'):
                print(f"   URL: {row.get('url', '')[:70]}...")

if __name__ == "__main__":
    filename = sys.argv[1] if len(sys.argv) > 1 else 'data/raw/hackernews_training.csv'
    num_rows = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    
    view_csv(filename, num_rows)