"""
populate_tension_scores.py - Calculate and store tension scores in database

Analyzes each story/comment for tension markers and stores scores
"""

import sys
import os
from datetime import datetime
from collections import defaultdict

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.database import Database


class TensionScorePopulator:
    """Calculate and populate tension scores"""
    
    # Tension markers (from tension_release.py)
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
        ],
        'future_focus': [
            'will', 'would', 'shall', 'going', 'expected', 'anticipated',
            'forecast', 'predicted', 'projected', 'upcoming', 'next', 'future'
        ],
        'questions': [
            'what', 'when', 'where', 'why', 'how', 'who', 'which'
        ]
    }
    
    RELEASE_MARKERS = {
        'resolution': [
            'announced', 'confirmed', 'revealed', 'disclosed', 'released',
            'launched', 'completed', 'finished', 'concluded', 'resolved'
        ],
        'past_tense': [
            'was', 'were', 'had', 'did', 'happened', 'occurred', 'took',
            'became', 'went', 'came', 'made', 'said', 'told'
        ],
        'definitives': [
            'confirmed', 'official', 'definitely', 'certainly', 'finally',
            'actually', 'indeed', 'truly', 'proven', 'verified'
        ]
    }
    
    # Weights for tension calculation
    TENSION_WEIGHTS = {
        'uncertainty': 1.5,
        'urgency': 2.0,
        'conflict': 1.8,
        'future_focus': 1.0,
        'questions': 0.8
    }
    
    RELEASE_WEIGHTS = {
        'resolution': 2.0,
        'past_tense': 1.2,
        'definitives': 1.5
    }
    
    def __init__(self, db_path: str = "data/linguistic_predictor.db"):
        """Initialize populator"""
        self.db = Database(db_path)
        
        # Flatten marker sets
        self.all_tension_markers = set()
        for markers in self.TENSION_MARKERS.values():
            self.all_tension_markers.update(markers)
        
        self.all_release_markers = set()
        for markers in self.RELEASE_MARKERS.values():
            self.all_release_markers.update(markers)
    
    def calculate_tension_score(self, words: str) -> dict:
        """
        Calculate tension score for a story/comment
        
        Args:
            words: Pipe-separated words
            
        Returns:
            Dictionary with tension metrics
        """
        if not words:
            return {
                'tension_score': 0.0,
                'release_score': 0.0,
                'net_tension': 0.0,
                'uncertainty_count': 0,
                'urgency_count': 0,
                'conflict_count': 0
            }
        
        word_list = words.split('|')
        word_set = set(word_list)
        total_words = len(word_list)
        
        if total_words == 0:
            return {
                'tension_score': 0.0,
                'release_score': 0.0,
                'net_tension': 0.0,
                'uncertainty_count': 0,
                'urgency_count': 0,
                'conflict_count': 0
            }
        
        # Count tension markers by category
        tension_counts = defaultdict(int)
        for category, markers in self.TENSION_MARKERS.items():
            found = word_set & set(markers)
            for word in found:
                tension_counts[category] += word_list.count(word)
        
        # Count release markers
        release_counts = defaultdict(int)
        for category, markers in self.RELEASE_MARKERS.items():
            found = word_set & set(markers)
            for word in found:
                release_counts[category] += word_list.count(word)
        
        # Calculate weighted tension score (normalized per 1000 words)
        tension_score = sum(
            (count / total_words) * 1000 * self.TENSION_WEIGHTS.get(cat, 1.0)
            for cat, count in tension_counts.items()
        )
        
        # Calculate weighted release score
        release_score = sum(
            (count / total_words) * 1000 * self.RELEASE_WEIGHTS.get(cat, 1.0)
            for cat, count in release_counts.items()
        )
        
        # Net tension
        net_tension = tension_score - release_score
        
        return {
            'tension_score': round(tension_score, 2),
            'release_score': round(release_score, 2),
            'net_tension': round(net_tension, 2),
            'uncertainty_count': tension_counts['uncertainty'],
            'urgency_count': tension_counts['urgency'],
            'conflict_count': tension_counts['conflict']
        }
    
    def populate_all_scores(self, batch_size: int = 1000):
        """
        Calculate and store tension scores for all records
        
        Args:
            batch_size: Number of records to process before committing
        """
        print("\n" + "=" * 80)
        print("POPULATING TENSION SCORES")
        print("=" * 80)
        
        # Get all stories with processed text
        query = """
            SELECT s.id, p.words, s.created_at
            FROM stories s
            JOIN processed_text p ON s.id = p.story_id
            WHERE s.id NOT IN (SELECT story_id FROM tension_scores)
        """
        
        self.db.cursor.execute(query)
        records = self.db.cursor.fetchall()
        
        total = len(records)
        print(f"Records to process: {total}")
        
        if total == 0:
            print("All records already have tension scores!")
            return
        
        processed = 0
        
        for i, record in enumerate(records, 1):
            story_id = record[0]
            words = record[1]
            created_at = record[2]
            
            # Calculate scores
            scores = self.calculate_tension_score(words)
            
            # Insert into tension_scores table
            self.db.cursor.execute("""
                INSERT INTO tension_scores 
                (story_id, tension_score, release_score, net_tension, 
                 uncertainty_count, urgency_count, conflict_count, calculated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                story_id,
                scores['tension_score'],
                scores['release_score'],
                scores['net_tension'],
                scores['uncertainty_count'],
                scores['urgency_count'],
                scores['conflict_count'],
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
            
            processed += 1
            
            # Commit in batches
            if processed % batch_size == 0:
                self.db.conn.commit()
                progress = (processed / total) * 100
                print(f"  Progress: {progress:.1f}% ({processed}/{total})", end='\r')
        
        # Final commit
        self.db.conn.commit()
        
        print(f"\n✓ Processed {processed} records")
        print("=" * 80)
    
    def show_stats(self):
        """Show tension score statistics"""
        print("\n" + "=" * 80)
        print("TENSION SCORE STATISTICS")
        print("=" * 80)
        
        # Count total
        self.db.cursor.execute("SELECT COUNT(*) FROM tension_scores")
        total = self.db.cursor.fetchone()[0]
        print(f"Total records with tension scores: {total}")
        
        # Average scores
        self.db.cursor.execute("""
            SELECT 
                AVG(tension_score),
                AVG(release_score),
                AVG(net_tension)
            FROM tension_scores
        """)
        avgs = self.db.cursor.fetchone()
        print(f"\nAverage scores:")
        print(f"  Tension:  {avgs[0]:.2f}")
        print(f"  Release:  {avgs[1]:.2f}")
        print(f"  Net:      {avgs[2]:.2f}")
        
        # Top tension records
        self.db.cursor.execute("""
            SELECT s.id, s.title, ts.net_tension, s.created_at
            FROM tension_scores ts
            JOIN stories s ON ts.story_id = s.id
            ORDER BY ts.net_tension DESC
            LIMIT 10
        """)
        
        print(f"\nTop 10 highest net tension:")
        print(f"{'Date':<12} {'Net':<8} {'Title':<60}")
        print("-" * 80)
        for row in self.db.cursor.fetchall():
            date = row[3][:10] if row[3] else 'Unknown'
            net = row[2]
            title = row[1][:55] if row[1] else 'No title'
            print(f"{date:<12} {net:<8.2f} {title:<60}")
        
        print("=" * 80)
    
    def close(self):
        """Close database connection"""
        self.db.close()


# Main execution
if __name__ == "__main__":
    print("=" * 80)
    print("TENSION SCORE POPULATOR")
    print("=" * 80)
    
    populator = TensionScorePopulator()
    
    # Populate scores
    populator.populate_all_scores()
    
    # Show statistics
    populator.show_stats()
    
    populator.close()
    
    print("\n✅ Tension scores populated!")