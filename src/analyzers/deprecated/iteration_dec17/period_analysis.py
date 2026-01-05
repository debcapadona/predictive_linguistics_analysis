"""
period_analysis.py - Comprehensive period analysis with CSV outputs

Analyzes a specific date range and outputs multiple CSV files
"""

import sys
import os
import csv
from datetime import datetime
from collections import Counter

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.database import Database


class PeriodAnalyzer:
    """Analyze a specific time period"""
    
    def __init__(self, start_date: str, end_date: str, 
                 output_dir: str = "data/analysis"):
        """
        Initialize analyzer
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            output_dir: Where to save CSV files
        """
        self.start_date = start_date
        self.end_date = end_date
        self.output_dir = output_dir
        self.db = Database()
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename prefix
        start_str = start_date.replace('-', '')
        end_str = end_date.replace('-', '')
        self.prefix = f"{start_str}_to_{end_str}"
    
    def analyze_top_words(self, limit: int = 100):
        """Export top words to CSV"""
        print(f"\n📊 Analyzing top words...")
        
        self.db.cursor.execute('''
            SELECT p.words
            FROM stories s
            JOIN processed_text p ON s.id = p.story_id
            WHERE s.created_at >= ? AND s.created_at < ?
            AND s.content_type = 'header'
        ''', (self.start_date, self.end_date))
        
        all_words = []
        for row in self.db.cursor.fetchall():
            if row[0]:
                words = row[0].split('|')
                all_words.extend(words)
        
        word_counts = Counter(all_words)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, f"{self.prefix}_top_words.csv")
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['word', 'count', 'rank'])
            for rank, (word, count) in enumerate(word_counts.most_common(limit), 1):
                writer.writerow([word, count, rank])
        
        print(f"✓ Saved: {output_file}")
        return output_file
    
    def analyze_tension_stories(self, limit: int = 100):
        """Export high tension stories to CSV"""
        print(f"\n🔥 Analyzing high tension stories...")
        
        self.db.cursor.execute('''
            SELECT s.id, s.title, s.created_at, ts.tension_score, 
                   ts.release_score, ts.net_tension,
                   ts.uncertainty_count, ts.urgency_count, ts.conflict_count
            FROM stories s
            JOIN tension_scores ts ON s.id = ts.story_id
            WHERE s.created_at >= ? AND s.created_at < ?
            AND s.content_type = 'header'
            ORDER BY ts.net_tension DESC
            LIMIT ?
        ''', (self.start_date, self.end_date, limit))
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, f"{self.prefix}_high_tension.csv")
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['story_id', 'title', 'date', 'tension_score', 
                           'release_score', 'net_tension', 'uncertainty_count',
                           'urgency_count', 'conflict_count'])
            for row in self.db.cursor.fetchall():
                writer.writerow(row)
        
        print(f"✓ Saved: {output_file}")
        return output_file
    
    def analyze_temporal_markers(self):
        """Export temporal markers to CSV"""
        print(f"\n⏰ Analyzing temporal markers...")
        
        self.db.cursor.execute('''
            SELECT tm.marker_text, tm.marker_type, tm.context_words, 
                   tm.predicted_timeframe, s.created_at, s.title
            FROM temporal_markers tm
            JOIN stories s ON tm.story_id = s.id
            WHERE s.created_at >= ? AND s.created_at < ?
            ORDER BY s.created_at
        ''', (self.start_date, self.end_date))
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, f"{self.prefix}_temporal_markers.csv")
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['marker_text', 'marker_type', 'context', 
                           'predicted_timeframe', 'date', 'story_title'])
            for row in self.db.cursor.fetchall():
                writer.writerow(row)
        
        print(f"✓ Saved: {output_file}")
        
        # Also create summary by marker type
        self.db.cursor.execute('''
            SELECT tm.marker_text, COUNT(*) as count
            FROM temporal_markers tm
            JOIN stories s ON tm.story_id = s.id
            WHERE s.created_at >= ? AND s.created_at < ?
            GROUP BY tm.marker_text
            ORDER BY count DESC
        ''', (self.start_date, self.end_date))
        
        summary_file = os.path.join(self.output_dir, f"{self.prefix}_temporal_summary.csv")
        with open(summary_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['marker_text', 'count'])
            for row in self.db.cursor.fetchall():
                writer.writerow(row)
        
        print(f"✓ Saved: {summary_file}")
        return output_file
    
    def analyze_cooccurrences(self, limit: int = 100):
        """Export word co-occurrences to CSV"""
        print(f"\n🔗 Analyzing word co-occurrences...")
        
        self.db.cursor.execute('''
            SELECT word1, word2, SUM(cooccurrence_count) as total_count
            FROM word_cooccurrences
            WHERE date_start >= ? AND date_end < ?
            GROUP BY word1, word2
            ORDER BY total_count DESC
            LIMIT ?
        ''', (self.start_date, self.end_date, limit))
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, f"{self.prefix}_cooccurrences.csv")
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['tension_marker', 'cooccurring_word', 'count'])
            for row in self.db.cursor.fetchall():
                writer.writerow(row)
        
        print(f"✓ Saved: {output_file}")
        return output_file
    
    def analyze_topics(self):
        """Export topic classifications to CSV"""
        print(f"\n🏷️  Analyzing topics...")
        
        # Check if any topics exist
        self.db.cursor.execute('''
            SELECT COUNT(*)
            FROM topic_classifications tc
            JOIN stories s ON tc.story_id = s.id
            WHERE s.created_at >= ? AND s.created_at < ?
            AND tc.iteration_1 IS NOT NULL
        ''', (self.start_date, self.end_date))
        
        count = self.db.cursor.fetchone()[0]
        
        if count == 0:
            print("  ⚠️  No topics classified for this period yet")
            return None
        
        # Get all classified stories in period
        self.db.cursor.execute('''
            SELECT s.id, s.title, s.created_at,
                   tc.iteration_1, tc.confidence_1,
                   tc.iteration_2, tc.confidence_2,
                   tc.iteration_3, tc.confidence_3,
                   tc.iteration_4, tc.confidence_4
            FROM topic_classifications tc
            JOIN stories s ON tc.story_id = s.id
            WHERE s.created_at >= ? AND s.created_at < ?
            ORDER BY s.created_at
        ''', (self.start_date, self.end_date))
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, f"{self.prefix}_topics.csv")
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['story_id', 'title', 'date', 
                           'topic_iter1', 'confidence_1',
                           'topic_iter2', 'confidence_2',
                           'topic_iter3', 'confidence_3',
                           'topic_iter4', 'confidence_4'])
            for row in self.db.cursor.fetchall():
                writer.writerow(row)
        
        print(f"✓ Saved: {output_file}")
        
        # Also create topic summary
        self.db.cursor.execute('''
            SELECT tc.iteration_4, COUNT(*) as count
            FROM topic_classifications tc
            JOIN stories s ON tc.story_id = s.id
            WHERE s.created_at >= ? AND s.created_at < ?
            AND tc.iteration_4 IS NOT NULL
            GROUP BY tc.iteration_4
            ORDER BY count DESC
        ''', (self.start_date, self.end_date))
        
        summary_file = os.path.join(self.output_dir, f"{self.prefix}_topic_summary.csv")
        with open(summary_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['topic', 'count'])
            for row in self.db.cursor.fetchall():
                writer.writerow(row)
        
        print(f"✓ Saved: {summary_file}")
        return output_file
    
    def run_full_analysis(self):
        """Run all analyses and export CSVs"""
        print("="*80)
        print(f"PERIOD ANALYSIS: {self.start_date} to {self.end_date}")
        print("="*80)
        
        # Run all analyses
        self.analyze_top_words()
        self.analyze_tension_stories()
        self.analyze_temporal_markers()
        self.analyze_cooccurrences()
        self.analyze_topics()
        
        print("\n" + "="*80)
        print("✅ ANALYSIS COMPLETE")
        print("="*80)
        print(f"Output directory: {self.output_dir}")
        print(f"File prefix: {self.prefix}")
        print("\nGenerated files:")
        print(f"  - {self.prefix}_top_words.csv")
        print(f"  - {self.prefix}_high_tension.csv")
        print(f"  - {self.prefix}_temporal_markers.csv")
        print(f"  - {self.prefix}_temporal_summary.csv")
        print(f"  - {self.prefix}_cooccurrences.csv")
        print(f"  - {self.prefix}_topics.csv (if available)")
        print(f"  - {self.prefix}_topic_summary.csv (if available)")
        print("="*80)
    
    def close(self):
        """Close database connection"""
        self.db.close()


# Main execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python3 -m src.analyzers.period_analysis START_DATE END_DATE [OUTPUT_DIR]")
        print("\nExample: python3 -m src.analyzers.period_analysis 2024-04-01 2024-07-01")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    output_dir = sys.argv[3] if len(sys.argv) > 3 else "data/analysis"
    
    analyzer = PeriodAnalyzer(start_date, end_date, output_dir)
    analyzer.run_full_analysis()
    analyzer.close()