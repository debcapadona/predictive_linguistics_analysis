"""
word_entropy_watchlist.py - Word-level entropy analysis with watchlist support

Calculate entropy scores for individual words PLUS always track watchlist words
regardless of frequency thresholds
"""

import csv
import math
import os
import yaml
from collections import Counter, defaultdict
from datetime import datetime
from typing import List, Dict, Tuple, Set
import sys

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.period_manager import PeriodManager
from src.core.stats_calculator import StatsCalculator
from src.core.text_processor import TextProcessor


class WordEntropyWatchlistAnalyzer:
    """Analyze entropy at the word level with watchlist support"""
    
    def __init__(self, config_path: str, watchlist_path: str = None):
        """
        Initialize analyzer with configuration and optional watchlist
        
        Args:
            config_path: Path to YAML configuration file
            watchlist_path: Path to watchlist YAML (optional)
        """
        print(f"Loading configuration from: {config_path}")
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Load watchlist if provided
        self.watchlist = set()
        self.watchlist_settings = {}
        
        if watchlist_path and os.path.exists(watchlist_path):
            print(f"Loading watchlist from: {watchlist_path}")
            with open(watchlist_path, 'r') as f:
                watchlist_config = yaml.safe_load(f)
                self.watchlist = set(watchlist_config.get('watchlist', []))
                self.watchlist_settings = watchlist_config.get('watchlist_settings', {})
            print(f"✓ Watchlist loaded: {len(self.watchlist)} words")
        else:
            print("No watchlist provided - using frequency thresholds only")
        
        self.text_processor = TextProcessor()
        print("✓ Word Entropy Watchlist Analyzer initialized")
    
    def load_processed_data(self, filename: str) -> List[Dict]:
        """Load processed data from CSV"""
        print(f"Loading data from: {filename}")
        
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
    
    def calculate_distribution_entropy(self, word: str, period_items: List[Dict]) -> float:
        """Calculate how evenly a word is distributed across stories"""
        story_contains_word = []
        
        for item in period_items:
            words = item.get('words', '').split('|') if item.get('words') else []
            contains = 1 if word in words else 0
            story_contains_word.append(contains)
        
        if not story_contains_word or sum(story_contains_word) == 0:
            return 0.0
        
        prob_appears = sum(story_contains_word) / len(story_contains_word)
        prob_not_appears = 1 - prob_appears
        
        entropy = 0.0
        if prob_appears > 0:
            entropy -= prob_appears * math.log2(prob_appears)
        if prob_not_appears > 0:
            entropy -= prob_not_appears * math.log2(prob_not_appears)
        
        return entropy
    
    def calculate_temporal_entropy(self, word: str, periods: List[Dict]) -> float:
        """Calculate how unpredictable a word's frequency is over time"""
        counts = []
        for period in periods:
            period_words = []
            for item in period['items']:
                words = item.get('words', '').split('|') if item.get('words') else []
                period_words.extend(words)
            
            word_count = period_words.count(word)
            counts.append(word_count)
        
        total = sum(counts)
        if total == 0:
            return 0.0
        
        entropy = 0.0
        for count in counts:
            if count > 0:
                prob = count / total
                entropy -= prob * math.log2(prob)
        
        return entropy
    
    def calculate_context_entropy(self, word: str, period_items: List[Dict]) -> float:
        """Calculate diversity of contexts where word appears"""
        context_words = []
        
        for item in period_items:
            words = item.get('words', '').split('|') if item.get('words') else []
            
            for i, w in enumerate(words):
                if w == word:
                    start = max(0, i - 2)
                    end = min(len(words), i + 3)
                    
                    context = words[start:i] + words[i+1:end]
                    context_words.extend(context)
        
        if not context_words:
            return 0.0
        
        word_counts = Counter(context_words)
        total = len(context_words)
        
        entropy = 0.0
        for count in word_counts.values():
            if count > 0:
                prob = count / total
                entropy -= prob * math.log2(prob)
        
        return entropy
    
    def calculate_cooccurrence_entropy(self, word: str, period_items: List[Dict]) -> float:
        """Calculate unpredictability of words that co-occur with this word"""
        cooccurring_words = []
        
        for item in period_items:
            words = item.get('words', '').split('|') if item.get('words') else []
            
            if word in words:
                other_words = [w for w in words if w != word]
                cooccurring_words.extend(other_words)
        
        if not cooccurring_words:
            return 0.0
        
        word_counts = Counter(cooccurring_words)
        total = len(cooccurring_words)
        
        entropy = 0.0
        for count in word_counts.values():
            if count > 0:
                prob = count / total
                entropy -= prob * math.log2(prob)
        
        return entropy
    
    def analyze_word(self, word: str, periods: List[Dict], is_watchlist: bool = False) -> Dict:
        """
        Calculate all entropy metrics for a word
        
        Args:
            word: Word to analyze
            periods: All periods
            is_watchlist: Whether this is a watchlist word
            
        Returns:
            Dictionary with all metrics
        """
        # Get frequency per period
        counts = []
        for period in periods:
            period_words = []
            for item in period['items']:
                words = item.get('words', '').split('|') if item.get('words') else []
                period_words.extend(words)
            
            word_count = period_words.count(word)
            counts.append(word_count)
        
        # Calculate z-scores and velocity
        stats = StatsCalculator.calculate_full_stats(counts, baseline_periods=1)
        
        # Calculate entropy metrics for monitoring period
        monitoring_period = periods[1]
        
        metrics = {
            'word': word,
            'is_watchlist': is_watchlist,
            'baseline_count': counts[0],
            'monitoring_count': counts[1],
            'event_count': counts[2],
            'max_z_score': stats['max_z_score'],
            'velocity': stats['velocity'],
            'acceleration': stats['acceleration'],
            'distribution_entropy': self.calculate_distribution_entropy(word, monitoring_period['items']),
            'temporal_entropy': self.calculate_temporal_entropy(word, periods),
            'context_entropy': self.calculate_context_entropy(word, monitoring_period['items']),
            'cooccurrence_entropy': self.calculate_cooccurrence_entropy(word, monitoring_period['items']),
            'signal_strength': StatsCalculator.get_signal_strength(stats['max_z_score'])
        }
        
        return metrics
    
    def run_analysis(self):
        """Run complete word-level entropy analysis with watchlist"""
        print("\n" + "=" * 120)
        print("WORD-LEVEL ENTROPY ANALYSIS WITH WATCHLIST")
        print("=" * 120)
        
        # Load data
        input_file = self.config['files']['input']
        items = self.load_processed_data(input_file)
        
        # Create periods
        period_definitions = PeriodManager.create_periods_from_config(self.config)
        periods = PeriodManager.create_period_objects(items, period_definitions)
        
        print("\nPeriods:")
        for period in periods:
            print(f"  {period['label']}: {period['item_count']} items")
        
        # Get all words from monitoring period
        print("\nExtracting words from monitoring period...")
        monitoring_words = []
        for item in periods[1]['items']:
            words = item.get('words', '').split('|') if item.get('words') else []
            monitoring_words.extend(words)
        
        word_counts = Counter(monitoring_words)
        
        # Separate into watchlist and regular words
        watchlist_words_found = []
        regular_words = []
        
        min_freq = self.config['parameters'].get('min_word_freq', 5)
        min_watchlist_freq = self.watchlist_settings.get('min_occurrences', 1)
        
        # Check all unique words
        for word, count in word_counts.items():
            if word in self.watchlist:
                if count >= min_watchlist_freq:
                    watchlist_words_found.append(word)
                    print(f"  🎯 Watchlist word found: '{word}' (count: {count})")
            elif count >= min_freq:
                regular_words.append(word)
        
        print(f"\nFound {len(watchlist_words_found)} watchlist words")
        print(f"Found {len(regular_words)} regular words (freq >= {min_freq})")
        
        # Analyze watchlist words
        watchlist_metrics = []
        if watchlist_words_found:
            print("\nAnalyzing watchlist words...")
            for word in watchlist_words_found:
                metrics = self.analyze_word(word, periods, is_watchlist=True)
                watchlist_metrics.append(metrics)
        
        # Analyze regular words
        print("\nAnalyzing regular words...")
        regular_metrics = []
        total = len(regular_words)
        for i, word in enumerate(regular_words, 1):
            if i % 50 == 0:
                progress = (i / total) * 100
                print(f"  Progress: {progress:.1f}% ({i}/{total})")
            
            metrics = self.analyze_word(word, periods, is_watchlist=False)
            regular_metrics.append(metrics)
        
        # Combine and sort
        all_metrics = watchlist_metrics + regular_metrics
        all_metrics.sort(key=lambda x: x['max_z_score'], reverse=True)
        
        # Display results
        self.display_results(watchlist_metrics, regular_metrics)
        
        # Save to CSV
        self.save_results(all_metrics)
    
    def display_results(self, watchlist_metrics: List[Dict], regular_metrics: List[Dict]):
        """Display word entropy results with watchlist highlighted"""
        print("\n" + "=" * 120)
        print("WORD ENTROPY RESULTS")
        print("=" * 120)
        
        # Show watchlist first
        if watchlist_metrics:
            print("\n🎯 WATCHLIST WORDS (tracked regardless of frequency):")
            print("-" * 120)
            
            header = f"{'Word':<20}{'Base':>8}{'Mon':>8}{'Event':>8}{'Z-Score':>10}{'Vel':>6}"
            header += f"{'Dist':>8}{'Temp':>8}{'Ctx':>8}{'CoOc':>8}{'Signal':>12}"
            print(header)
            print("-" * 120)
            
            for metrics in sorted(watchlist_metrics, key=lambda x: x['max_z_score'], reverse=True):
                row = f"{metrics['word']:<20}"
                row += f"{metrics['baseline_count']:>8}"
                row += f"{metrics['monitoring_count']:>8}"
                row += f"{metrics['event_count']:>8}"
                row += f"{metrics['max_z_score']:>10.2f}"
                row += f"{metrics['velocity']:>6}"
                row += f"{metrics['distribution_entropy']:>8.3f}"
                row += f"{metrics['temporal_entropy']:>8.3f}"
                row += f"{metrics['context_entropy']:>8.3f}"
                row += f"{metrics['cooccurrence_entropy']:>8.3f}"
                
                emoji = StatsCalculator.get_signal_emoji(metrics['max_z_score'])
                row += f"{emoji} {metrics['signal_strength']:>9}"
                
                print(row)
        
        # Show top regular words
        print("\n" + "=" * 120)
        print("TOP REGULAR WORDS BY Z-SCORE:")
        print("-" * 120)
        
        header = f"{'Word':<20}{'Base':>8}{'Mon':>8}{'Event':>8}{'Z-Score':>10}{'Vel':>6}"
        header += f"{'Dist':>8}{'Temp':>8}{'Ctx':>8}{'CoOc':>8}{'Signal':>12}"
        print(header)
        print("-" * 120)
        
        top_n = self.config['parameters'].get('top_n_words', 40)
        for metrics in sorted(regular_metrics, key=lambda x: x['max_z_score'], reverse=True)[:top_n]:
            row = f"{metrics['word']:<20}"
            row += f"{metrics['baseline_count']:>8}"
            row += f"{metrics['monitoring_count']:>8}"
            row += f"{metrics['event_count']:>8}"
            row += f"{metrics['max_z_score']:>10.2f}"
            row += f"{metrics['velocity']:>6}"
            row += f"{metrics['distribution_entropy']:>8.3f}"
            row += f"{metrics['temporal_entropy']:>8.3f}"
            row += f"{metrics['context_entropy']:>8.3f}"
            row += f"{metrics['cooccurrence_entropy']:>8.3f}"
            
            emoji = StatsCalculator.get_signal_emoji(metrics['max_z_score'])
            row += f"{emoji} {metrics['signal_strength']:>9}"
            
            print(row)
        
        print("=" * 120)
    
    def save_results(self, word_metrics: List[Dict]):
        """Save results to CSV"""
        output_dir = self.config['files']['output_dir']
        output_prefix = self.config['files']['output_prefix']
        output_file = os.path.join(output_dir, f"{output_prefix}_word_entropy_watchlist.csv")
        
        os.makedirs(output_dir, exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'word', 'is_watchlist', 'baseline_count', 'monitoring_count', 'event_count',
                'max_z_score', 'velocity', 'acceleration',
                'distribution_entropy', 'temporal_entropy', 'context_entropy',
                'cooccurrence_entropy', 'signal_strength'
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(word_metrics)
        
        print(f"\n✅ Word entropy analysis with watchlist saved to: {output_file}")


# Main execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python3 -m src.analyzers.word_entropy_watchlist <config_file.yaml> [watchlist.yaml]")
        print("\nExample: python3 -m src.analyzers.word_entropy_watchlist configs/experiments/may_july_2024.yaml configs/watchlist.yaml")
        sys.exit(1)
    
    config_path = sys.argv[1]
    watchlist_path = sys.argv[2] if len(sys.argv) > 2 else None
    
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    analyzer = WordEntropyWatchlistAnalyzer(config_path, watchlist_path)
    analyzer.run_analysis()