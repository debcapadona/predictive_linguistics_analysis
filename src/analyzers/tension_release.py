"""
tension_release.py - Tension & Release linguistic curve analysis

Tracks linguistic patterns that indicate building tension before events
and release/resolution after events occur.

Tension indicators: uncertainty, urgency, conflict, future-focused language
Release indicators: past tense, definitive language, resolution
"""

import csv
import os
import yaml
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import sys

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.period_manager import PeriodManager
from src.core.stats_calculator import StatsCalculator


class TensionReleaseAnalyzer:
    """Analyze tension and release patterns in language"""
    
    # Define linguistic markers
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
    
    def __init__(self, config_path: str):
        """
        Initialize analyzer with configuration
        
        Args:
            config_path: Path to YAML configuration file
        """
        print(f"Loading configuration from: {config_path}")
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Flatten marker sets
        self.all_tension_markers = set()
        for markers in self.TENSION_MARKERS.values():
            self.all_tension_markers.update(markers)
        
        self.all_release_markers = set()
        for markers in self.RELEASE_MARKERS.values():
            self.all_release_markers.update(markers)
        
        print(f"✓ Tension/Release Analyzer initialized")
        print(f"  Tracking {len(self.all_tension_markers)} tension markers")
        print(f"  Tracking {len(self.all_release_markers)} release markers")
    
    def load_processed_data(self, filename: str) -> List[Dict]:
        """Load processed data from CSV"""
        print(f"\nLoading data from: {filename}")
        
        items = []
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    row['date'] = datetime.strptime(row['created_at'], '%Y-%m-%d %H:%M:%S')
                    items.append(row)
                except:
                    continue
        
        print(f"Loaded {len(items)} items")
        return items
    
    def calculate_tension_score(self, items: List[Dict]) -> Dict[str, float]:
        """
        Calculate tension score for a set of items
        
        Args:
            items: List of items to analyze
            
        Returns:
            Dictionary with tension metrics
        """
        tension_counts = defaultdict(int)
        total_words = 0
        
        for item in items:
            words = item.get('words', '').split('|') if item.get('words') else []
            words_set = set(words)
            total_words += len(words)
            
            # Count tension markers by category
            for category, markers in self.TENSION_MARKERS.items():
                found = words_set & set(markers)
                tension_counts[category] += len(found)
        
        # Normalize by total words (per 1000 words)
        if total_words > 0:
            normalized = {
                cat: (count / total_words) * 1000 
                for cat, count in tension_counts.items()
            }
        else:
            normalized = {cat: 0 for cat in self.TENSION_MARKERS.keys()}
        
        # Calculate overall tension score (weighted average)
        weights = {
            'uncertainty': 1.5,  # Uncertainty is strong tension signal
            'urgency': 2.0,      # Urgency is strongest
            'conflict': 1.8,     # Conflict is very strong
            'future_focus': 1.0, # Future focus is moderate
            'questions': 0.8     # Questions are mild tension
        }
        
        tension_score = sum(
            normalized[cat] * weights.get(cat, 1.0)
            for cat in normalized.keys()
        )
        
        return {
            'tension_score': tension_score,
            'by_category': normalized,
            'total_tension_markers': sum(tension_counts.values()),
            'total_words': total_words
        }
    
    def calculate_release_score(self, items: List[Dict]) -> Dict[str, float]:
        """
        Calculate release score for a set of items
        
        Args:
            items: List of items to analyze
            
        Returns:
            Dictionary with release metrics
        """
        release_counts = defaultdict(int)
        total_words = 0
        
        for item in items:
            words = item.get('words', '').split('|') if item.get('words') else []
            words_set = set(words)
            total_words += len(words)
            
            # Count release markers by category
            for category, markers in self.RELEASE_MARKERS.items():
                found = words_set & set(markers)
                release_counts[category] += len(found)
        
        # Normalize by total words (per 1000 words)
        if total_words > 0:
            normalized = {
                cat: (count / total_words) * 1000 
                for cat, count in release_counts.items()
            }
        else:
            normalized = {cat: 0 for cat in self.RELEASE_MARKERS.keys()}
        
        # Calculate overall release score (weighted average)
        weights = {
            'resolution': 2.0,   # Resolution language strongest
            'past_tense': 1.2,   # Past tense moderate
            'definitives': 1.5   # Definitive language strong
        }
        
        release_score = sum(
            normalized[cat] * weights.get(cat, 1.0)
            for cat in normalized.keys()
        )
        
        return {
            'release_score': release_score,
            'by_category': normalized,
            'total_release_markers': sum(release_counts.values()),
            'total_words': total_words
        }
    
    def analyze_by_time_window(self, items: List[Dict], window_days: int = 7) -> List[Dict]:
        """
        Analyze tension/release over time windows
        
        Args:
            items: All items
            window_days: Size of time window in days
            
        Returns:
            List of time window analyses
        """
        if not items:
            return []
        
        # Sort by date
        items_sorted = sorted(items, key=lambda x: x['date'])
        
        start_date = items_sorted[0]['date']
        end_date = items_sorted[-1]['date']
        
        print(f"\nAnalyzing {window_days}-day windows from {start_date.date()} to {end_date.date()}")
        
        results = []
        current = start_date
        
        while current < end_date:
            window_end = current + timedelta(days=window_days)
            
            # Get items in this window
            window_items = [
                item for item in items_sorted
                if current <= item['date'] < window_end
            ]
            
            if window_items:
                tension = self.calculate_tension_score(window_items)
                release = self.calculate_release_score(window_items)
                
                # Calculate net tension (tension - release)
                net_tension = tension['tension_score'] - release['release_score']
                
                results.append({
                    'window_start': current,
                    'window_end': window_end,
                    'item_count': len(window_items),
                    'tension_score': tension['tension_score'],
                    'release_score': release['release_score'],
                    'net_tension': net_tension,
                    'tension_by_category': tension['by_category'],
                    'release_by_category': release['by_category']
                })
            
            current = window_end
        
        print(f"Generated {len(results)} time windows")
        return results
    
    def run_analysis(self):
        """Run complete tension/release analysis"""
        print("\n" + "=" * 120)
        print("TENSION & RELEASE CURVE ANALYSIS")
        print("=" * 120)
        
        # Load data
        input_file = self.config['files']['input']
        items = self.load_processed_data(input_file)
        
        # Get window size from config
        window_days = self.config['parameters'].get('window_days', 7)
        
        # Analyze by time windows
        time_series = self.analyze_by_time_window(items, window_days)
        
        # Display results
        self.display_results(time_series)
        
        # Save results
        self.save_results(time_series)
    
    def display_results(self, time_series: List[Dict]):
        """Display tension/release curve"""
        print("\n" + "=" * 120)
        print("TENSION/RELEASE CURVE")
        print("=" * 120)
        
        print(f"\n{'Window Start':<15}{'Items':<8}{'Tension':>10}{'Release':>10}{'Net':>10}{'Trend':>15}")
        print("-" * 120)
        
        for window in time_series:
            date_str = window['window_start'].strftime('%Y-%m-%d')
            tension = window['tension_score']
            release = window['release_score']
            net = window['net_tension']
            
            # Determine trend
            if net > 5:
                trend = "🔴 HIGH TENSION"
            elif net > 2:
                trend = "🟠 RISING"
            elif net > -2:
                trend = "🟡 BALANCED"
            elif net > -5:
                trend = "🟢 RELEASING"
            else:
                trend = "✅ RESOLVED"
            
            print(f"{date_str:<15}{window['item_count']:<8}{tension:>10.2f}{release:>10.2f}{net:>10.2f}{trend:>20}")
        
        print("=" * 120)
        
        # Show category breakdown for highest tension period
        if time_series:
            max_tension_window = max(time_series, key=lambda x: x['net_tension'])
            
            print(f"\n📊 PEAK TENSION PERIOD: {max_tension_window['window_start'].strftime('%Y-%m-%d')}")
            print(f"   Net Tension Score: {max_tension_window['net_tension']:.2f}")
            print(f"\n   Tension Breakdown:")
            for cat, score in max_tension_window['tension_by_category'].items():
                print(f"     {cat:15s}: {score:6.2f}")
            
            print(f"\n   Release Breakdown:")
            for cat, score in max_tension_window['release_by_category'].items():
                print(f"     {cat:15s}: {score:6.2f}")
        
        print("=" * 120)
    
    def save_results(self, time_series: List[Dict]):
        """Save results to CSV"""
        output_dir = self.config['files']['output_dir']
        output_prefix = self.config['files']['output_prefix']
        output_file = os.path.join(output_dir, f"{output_prefix}_tension_release.csv")
        
        os.makedirs(output_dir, exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'window_start', 'window_end', 'item_count',
                'tension_score', 'release_score', 'net_tension'
            ]
            
            # Add category columns
            if time_series:
                for cat in time_series[0]['tension_by_category'].keys():
                    fieldnames.append(f'tension_{cat}')
                for cat in time_series[0]['release_by_category'].keys():
                    fieldnames.append(f'release_{cat}')
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for window in time_series:
                row = {
                    'window_start': window['window_start'].strftime('%Y-%m-%d'),
                    'window_end': window['window_end'].strftime('%Y-%m-%d'),
                    'item_count': window['item_count'],
                    'tension_score': round(window['tension_score'], 2),
                    'release_score': round(window['release_score'], 2),
                    'net_tension': round(window['net_tension'], 2)
                }
                
                # Add categories
                for cat, score in window['tension_by_category'].items():
                    row[f'tension_{cat}'] = round(score, 2)
                for cat, score in window['release_by_category'].items():
                    row[f'release_{cat}'] = round(score, 2)
                
                writer.writerow(row)
        
        print(f"\n✅ Tension/Release curve saved to: {output_file}")


# Main execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python3 -m src.analyzers.tension_release <config_file.yaml>")
        print("\nExample: python3 -m src.analyzers.tension_release configs/experiments/may_july_2024.yaml")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    analyzer = TensionReleaseAnalyzer(config_path)
    analyzer.run_analysis()