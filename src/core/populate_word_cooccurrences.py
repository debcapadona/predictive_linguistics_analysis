"""
populate_word_cooccurrences.py - Extract word co-occurrences with tension markers

Tracks which words appear together with tension markers
Helps identify what people are worried/urgent about
"""

import sys
import os
from datetime import datetime
from collections import Counter, defaultdict
from typing import List, Dict, Tuple

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.database import Database


class WordCooccurrencePopulator:
    """Extract and populate word co-occurrences"""
    
    # Tension markers to track (from tension_release.py)
    TENSION_MARKERS = {
        'uncertainty': [
            'might', 'could', 'possibly', 'maybe', 'uncertain', 'unclear',
            'unknown', 'unsure', 'speculation', 'rumor', 'alleged', 'reportedly'
        ],
        'urgency': [
            'soon', 'imminent', 'approaching', 'coming', 'urgent', 'critical',
            'emergency', 'immediate', 'quickly', 'rapidly', 'accelerating'
        ],
        'conflict': [
            'crisis', 'threat', 'danger', 'risk', 'concern', 'worry', 'fear',
            'alarm', 'warning', 'trouble', 'problem', 'issue', 'conflict'
        ]
    }
    
    def __init__(self, db_path: str = "data/linguistic_predictor.db"):
        """Initialize populator"""
        self.db = Database(db_path)
        
        # Flatten all tension markers
        self.all_tension_markers = set()
        for markers in self.TENSION_MARKERS.values():
            self.all_tension_markers.update(markers)
    
    def extract_cooccurrences(self, words: str, window_size: int = 5) -> List[Tuple[str, str]]:
        """
        Extract word pairs that co-occur within a window
        Only pairs where at least one word is a tension marker
        
        Args:
            words: Pipe-separated words
            window_size: How many words apart to consider co-occurring
            
        Returns:
            List of (word1, word2) tuples
        """
        if not words:
            return []
        
        word_list = words.split('|')
        cooccurrences = []
        
        # Find positions of tension markers
        tension_positions = [
            i for i, word in enumerate(word_list)
            if word in self.all_tension_markers
        ]
        
        # For each tension marker, get nearby words
        for tension_pos in tension_positions:
            tension_word = word_list[tension_pos]
            
            # Look within window
            start = max(0, tension_pos - window_size)
            end = min(len(word_list), tension_pos + window_size + 1)
            
            for i in range(start, end):
                if i == tension_pos:
                    continue
                
                nearby_word = word_list[i]
                
                # Skip if nearby word is also a tension marker
                # (we want content words, not marker-marker pairs)
                if nearby_word in self.all_tension_markers:
                    continue
                
                # Skip very short words
                if len(nearby_word) < 3:
                    continue
                
                # Always put tension marker first for consistency
                cooccurrences.append((tension_word, nearby_word))
        
    def __init__(self, db_path: str = "data/linguistic_predictor.db"):
        """Initialize populator"""
        self.db = Database(db_path)
        
        # Flatten all tension markers
        self.all_tension_markers = set()
        for markers in self.TENSION_MARKERS.values():
            self.all_tension_markers.update(markers)
        
        # Load custom stopwords
        self.custom_stopwords = self.load_stopwords()
    
    def load_stopwords(self) -> set:
        """Load custom stopwords from file"""
        stopwords_file = "configs/stopwords.txt"
        stopwords = set()
        
        if os.path.exists(stopwords_file):
            with open(stopwords_file, 'r', encoding='utf-8') as f:
                for line in f:
                    word = line.strip().lower()
                    if word and not word.startswith('#'):
                        stopwords.add(word)
            print(f"✓ Loaded {len(stopwords)} custom stopwords")
        else:
            print("⚠️  No custom stopwords file found")
        
        return stopwords
    
    def extract_cooccurrences(self, words: str, window_size: int = 5) -> List[Tuple[str, str]]:
        """
        Extract word pairs that co-occur within a window
        Only pairs where at least one word is a tension marker
        
        Args:
            words: Pipe-separated words
            window_size: How many words apart to consider co-occurring
            
        Returns:
            List of (word1, word2) tuples
        """
        if not words:
            return []
        
        word_list = words.split('|')
        cooccurrences = []
        
        # Find positions of tension markers
        tension_positions = [
            i for i, word in enumerate(word_list)
            if word in self.all_tension_markers
        ]
        
        # For each tension marker, get nearby words
        for tension_pos in tension_positions:
            tension_word = word_list[tension_pos]
            
            # Look within window
            start = max(0, tension_pos - window_size)
            end = min(len(word_list), tension_pos + window_size + 1)
            
            for i in range(start, end):
                if i == tension_pos:
                    continue
                
                nearby_word = word_list[i]
                
                # Skip if nearby word is also a tension marker
                if nearby_word in self.all_tension_markers:
                    continue
                
                # Skip very short words
                if len(nearby_word) < 3:
                    continue
                
                # *** NEW: Skip custom stopwords ***
                if nearby_word in self.custom_stopwords:
                    continue
                
                # Always put tension marker first for consistency
                cooccurrences.append((tension_word, nearby_word))
        
        return cooccurrences
    
    def populate_cooccurrences(self, time_window: str = 'month', 
                               batch_size: int = 1000):
        """
        Calculate and store word co-occurrences
        
        Args:
            time_window: 'week', 'month', or 'year'
            batch_size: Number of time windows to process before committing
        """
        print("\n" + "=" * 80)
        print(f"POPULATING WORD CO-OCCURRENCES ({time_window} windows)")
        print("=" * 80)
        
        # Get date range from database
        self.db.cursor.execute("""
            SELECT MIN(created_at), MAX(created_at)
            FROM stories
        """)
        date_range = self.db.cursor.fetchone()
        
        if not date_range[0] or not date_range[1]:
            print("No stories in database!")
            return
        
        start_date = datetime.strptime(date_range[0][:10], '%Y-%m-%d')
        end_date = datetime.strptime(date_range[1][:10], '%Y-%m-%d')
        
        print(f"Date range: {start_date.date()} to {end_date.date()}")
        
        # Determine window size in days
        if time_window == 'week':
            window_days = 7
        elif time_window == 'month':
            window_days = 30
        else:  # year
            window_days = 365
        
        # Process in time windows
        current_start = start_date
        windows_processed = 0
        total_cooccurrences = 0
        
        while current_start < end_date:
            from datetime import timedelta
            current_end = current_start + timedelta(days=window_days)
            
            # Get stories in this window
            query = """
                SELECT s.id, p.words, s.source_id, s.content_type
                FROM stories s
                JOIN processed_text p ON s.id = p.story_id
                WHERE s.created_at >= ? AND s.created_at < ?
            """
            
            self.db.cursor.execute(query, (
                current_start.strftime('%Y-%m-%d'),
                current_end.strftime('%Y-%m-%d')
            ))
            
            window_records = self.db.cursor.fetchall()
            
            if window_records:
                # Aggregate co-occurrences for this window
                window_cooccurrences = Counter()
                content_types = defaultdict(Counter)
                sources = defaultdict(Counter)
                
                for record in window_records:
                    story_id, words, source_id, content_type = record
                    
                    # Extract co-occurrences
                    pairs = self.extract_cooccurrences(words)
                    
                    for pair in pairs:
                        window_cooccurrences[pair] += 1
                        content_types[pair][content_type] += 1
                        sources[pair][source_id] += 1
                
                # Store significant co-occurrences (appearing at least twice)
                for (word1, word2), count in window_cooccurrences.items():
                    if count >= 2:  # Threshold for significance
                        # Get most common content_type and source for this pair
                        most_common_content = content_types[(word1, word2)].most_common(1)[0][0]
                        most_common_source = sources[(word1, word2)].most_common(1)[0][0]
                        
                        self.db.cursor.execute("""
                            INSERT INTO word_cooccurrences
                            (word1, word2, cooccurrence_count, time_window,
                             date_start, date_end, source_id, content_type)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            word1,
                            word2,
                            count,
                            time_window,
                            current_start.strftime('%Y-%m-%d'),
                            current_end.strftime('%Y-%m-%d'),
                            most_common_source,
                            most_common_content
                        ))
                        
                        total_cooccurrences += 1
            
            windows_processed += 1
            
            # Commit in batches
            if windows_processed % batch_size == 0:
                self.db.conn.commit()
                print(f"  Processed {windows_processed} windows, found {total_cooccurrences} co-occurrences", end='\r')
            
            # Move to next window
            current_start = current_end
        
        # Final commit
        self.db.conn.commit()
        
        print(f"\n✓ Processed {windows_processed} time windows")
        print(f"✓ Found {total_cooccurrences} significant co-occurrences")
        print("=" * 80)
    
    def show_stats(self):
        """Show co-occurrence statistics"""
        print("\n" + "=" * 80)
        print("WORD CO-OCCURRENCE STATISTICS")
        print("=" * 80)
        
        # Count total
        self.db.cursor.execute("SELECT COUNT(*) FROM word_cooccurrences")
        total = self.db.cursor.fetchone()[0]
        print(f"Total co-occurrences stored: {total}")
        
        # Most common tension marker + word pairs
        self.db.cursor.execute("""
            SELECT word1, word2, SUM(cooccurrence_count) as total_count
            FROM word_cooccurrences
            GROUP BY word1, word2
            ORDER BY total_count DESC
            LIMIT 20
        """)
        
        print(f"\nTop 20 co-occurring pairs (tension marker + word):")
        print(f"{'Tension Marker':<20} {'Co-occurring Word':<20} {'Count':>10}")
        print("-" * 80)
        for row in self.db.cursor.fetchall():
            print(f"{row[0]:<20} {row[1]:<20} {row[2]:>10}")
        
        # Recent urgency co-occurrences
        self.db.cursor.execute("""
            SELECT word1, word2, cooccurrence_count, date_start
            FROM word_cooccurrences
            WHERE word1 IN ('soon', 'imminent', 'urgent', 'critical', 'emergency')
            ORDER BY date_start DESC
            LIMIT 15
        """)
        
        print(f"\nRecent urgency word co-occurrences:")
        print(f"{'Date':<12} {'Urgency Word':<15} {'With Word':<20} {'Count':>8}")
        print("-" * 80)
        for row in self.db.cursor.fetchall():
            date = row[3][:10] if row[3] else 'Unknown'
            print(f"{date:<12} {row[0]:<15} {row[1]:<20} {row[2]:>8}")
        
        print("=" * 80)
    
    def close(self):
        """Close database connection"""
        self.db.close()


# Main execution
if __name__ == "__main__":
    import sys
    
    time_window = 'month'  # Default
    
    if len(sys.argv) > 1:
        time_window = sys.argv[1]
        if time_window not in ['week', 'month', 'year']:
            print("Usage: python3 -m src.core.populate_word_cooccurrences [week|month|year]")
            sys.exit(1)
    
    print("=" * 80)
    print("WORD CO-OCCURRENCE POPULATOR")
    print("=" * 80)
    
    populator = WordCooccurrencePopulator()
    
    # Populate co-occurrences
    populator.populate_cooccurrences(time_window)
    
    # Show statistics
    populator.show_stats()
    
    populator.close()
    
    print(f"\n✅ Word co-occurrences populated for {time_window} windows!")