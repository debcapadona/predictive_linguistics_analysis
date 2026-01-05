"""
populate_temporal_markers.py - Extract and store temporal markers from text

Identifies temporal language that indicates WHEN something might happen
"""

import sys
import os
import re
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.database import Database


class TemporalMarkerPopulator:
    """Extract and populate temporal markers"""
    
    # Temporal marker patterns
    TEMPORAL_PATTERNS = {
        'urgency': [
            'soon', 'shortly', 'imminent', 'imminently', 'immediately',
            'right now', 'any moment', 'any day', 'any time'
        ],
        'relative_near': [
            'today', 'tomorrow', 'tonight', 'this week', 'this weekend',
            'next week', 'next month', 'next year', 'coming days', 'coming weeks',
            'coming months', 'in days', 'in weeks', 'within days', 'within weeks'
        ],
        'relative_medium': [
            'this quarter', 'next quarter', 'this year', 'next year',
            'in months', 'within months', 'few months', 'several months',
            'this spring', 'this summer', 'this fall', 'this winter',
            'next spring', 'next summer', 'next fall', 'next winter'
        ],
        'relative_far': [
            'in years', 'within years', 'few years', 'several years',
            'next decade', 'long term', 'eventually', 'someday'
        ],
        'deadline': [
            'by', 'before', 'until', 'deadline', 'due', 'expires',
            'ends', 'finishes', 'closes', 'concludes'
        ],
        'specific_date': [
            'january', 'february', 'march', 'april', 'may', 'june',
            'july', 'august', 'september', 'october', 'november', 'december',
            'q1', 'q2', 'q3', 'q4', 'h1', 'h2'
        ]
    }
    
    # Timeframe mappings
    TIMEFRAME_MAP = {
        'urgency': 'immediate (0-7 days)',
        'relative_near': 'near (1-4 weeks)',
        'relative_medium': 'medium (1-6 months)',
        'relative_far': 'far (6+ months)',
        'deadline': 'specific deadline',
        'specific_date': 'specific date'
    }
    
    def __init__(self, db_path: str = "data/linguistic_predictor.db"):
        """Initialize populator"""
        self.db = Database(db_path)
        
        # Compile patterns for efficiency
        self.compiled_patterns = {}
        for marker_type, markers in self.TEMPORAL_PATTERNS.items():
            # Create word boundary pattern for each marker
            patterns = [rf'\b{re.escape(marker)}\b' for marker in markers]
            self.compiled_patterns[marker_type] = re.compile(
                '|'.join(patterns), 
                re.IGNORECASE
            )
    
    def extract_context(self, text: str, marker_pos: int, window: int = 50) -> str:
        """
        Extract context words around a temporal marker
        
        Args:
            text: Full text
            marker_pos: Position of marker in text
            window: Character window on each side
            
        Returns:
            Context string
        """
        start = max(0, marker_pos - window)
        end = min(len(text), marker_pos + window)
        context = text[start:end]
        
        # Clean up
        context = ' '.join(context.split())
        return context
    
    def find_temporal_markers(self, story_id: str, text: str, 
                             created_at: str) -> List[Dict]:
        """
        Find all temporal markers in text
        
        Args:
            story_id: Story ID
            text: Text to analyze (title or comment)
            created_at: When story/comment was created
            
        Returns:
            List of marker dictionaries
        """
        if not text:
            return []
        
        markers = []
        
        for marker_type, pattern in self.compiled_patterns.items():
            matches = pattern.finditer(text)
            
            for match in matches:
                marker_text = match.group()
                marker_pos = match.start()
                
                # Get surrounding context
                context = self.extract_context(text, marker_pos)
                
                # Map to predicted timeframe
                predicted_timeframe = self.TIMEFRAME_MAP[marker_type]
                
                markers.append({
                    'story_id': story_id,
                    'marker_text': marker_text.lower(),
                    'marker_type': marker_type,
                    'context_words': context,
                    'predicted_timeframe': predicted_timeframe,
                    'created_at': created_at
                })
        
        return markers
    
    def populate_all_markers(self, batch_size: int = 1000):
        """
        Extract and store temporal markers for all records
        
        Args:
            batch_size: Number of records to process before committing
        """
        print("\n" + "=" * 80)
        print("POPULATING TEMPORAL MARKERS")
        print("=" * 80)
        
        # Get all stories that don't have markers yet
        # We'll process both title and processed text
        query = """
            SELECT s.id, s.title, s.created_at
            FROM stories s
            WHERE s.id NOT IN (
                SELECT DISTINCT story_id FROM temporal_markers
            )
        """
        
        self.db.cursor.execute(query)
        records = self.db.cursor.fetchall()
        
        total = len(records)
        print(f"Records to process: {total}")
        
        if total == 0:
            print("All records already have temporal markers extracted!")
            return
        
        processed = 0
        total_markers = 0
        
        for i, record in enumerate(records, 1):
            story_id = record[0]
            title = record[1] or ''
            created_at = record[2]
            
            # Find markers in title
            markers = self.find_temporal_markers(story_id, title, created_at)
            
            # Insert markers
            for marker in markers:
                self.db.cursor.execute("""
                    INSERT INTO temporal_markers 
                    (story_id, marker_text, marker_type, context_words, 
                     predicted_timeframe, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    marker['story_id'],
                    marker['marker_text'],
                    marker['marker_type'],
                    marker['context_words'],
                    marker['predicted_timeframe'],
                    marker['created_at']
                ))
                total_markers += 1
            
            processed += 1
            
            # Commit in batches
            if processed % batch_size == 0:
                self.db.conn.commit()
                progress = (processed / total) * 100
                print(f"  Progress: {progress:.1f}% ({processed}/{total}) - Found {total_markers} markers", end='\r')
        
        # Final commit
        self.db.conn.commit()
        
        print(f"\n✓ Processed {processed} records")
        print(f"✓ Found {total_markers} temporal markers")
        print("=" * 80)
    
    def show_stats(self):
        """Show temporal marker statistics"""
        print("\n" + "=" * 80)
        print("TEMPORAL MARKER STATISTICS")
        print("=" * 80)
        
        # Count total
        self.db.cursor.execute("SELECT COUNT(*) FROM temporal_markers")
        total = self.db.cursor.fetchone()[0]
        print(f"Total temporal markers: {total}")
        
        # By type
        self.db.cursor.execute("""
            SELECT marker_type, COUNT(*) as count
            FROM temporal_markers
            GROUP BY marker_type
            ORDER BY count DESC
        """)
        
        print(f"\nBy marker type:")
        for row in self.db.cursor.fetchall():
            print(f"  {row[0]:20s}: {row[1]:6d}")
        
        # Most common markers
        self.db.cursor.execute("""
            SELECT marker_text, COUNT(*) as count
            FROM temporal_markers
            GROUP BY marker_text
            ORDER BY count DESC
            LIMIT 15
        """)
        
        print(f"\nTop 15 most common markers:")
        for row in self.db.cursor.fetchall():
            print(f"  {row[0]:20s}: {row[1]:6d}")
        
        # Recent urgency markers
        self.db.cursor.execute("""
            SELECT tm.marker_text, tm.context_words, s.created_at
            FROM temporal_markers tm
            JOIN stories s ON tm.story_id = s.id
            WHERE tm.marker_type = 'urgency'
            ORDER BY s.created_at DESC
            LIMIT 10
        """)
        
        print(f"\nRecent urgency markers (last 10):")
        print(f"{'Date':<12} {'Marker':<15} {'Context':<50}")
        print("-" * 80)
        for row in self.db.cursor.fetchall():
            date = row[2][:10] if row[2] else 'Unknown'
            marker = row[0][:12]
            context = row[1][:45] + '...' if len(row[1]) > 45 else row[1]
            print(f"{date:<12} {marker:<15} {context:<50}")
        
        print("=" * 80)
    
    def close(self):
        """Close database connection"""
        self.db.close()


# Main execution
if __name__ == "__main__":
    print("=" * 80)
    print("TEMPORAL MARKER POPULATOR")
    print("=" * 80)
    
    populator = TemporalMarkerPopulator()
    
    # Populate markers
    populator.populate_all_markers()
    
    # Show statistics
    populator.show_stats()
    
    populator.close()
    
    print("\n✅ Temporal markers populated!")