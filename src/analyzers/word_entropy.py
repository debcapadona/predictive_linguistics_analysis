"""
word_entropy.py - Word-level entropy analysis

Calculate entropy scores for individual words to measure their information content,
predictability, and usage patterns
"""

import csv
import math
import os
import yaml
from collections import Counter, defaultdict
from datetime import datetime
from typing import List, Dict, Tuple
import sys

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.period_manager import PeriodManager
from src.core.stats_calculator import StatsCalculator
from src.core.text_processor import TextProcessor


class WordEntropyAnalyzer:
    """Analyze entropy at the word level"""
    
    def __init__(self, config_path: str):
        """
        Initialize analyzer with configuration
        
        Args:
            config_path: Path to YAML configuration file
        """
        print(f"Loading configuration from: {config_path}")
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.text_processor = TextProcessor()
        print("✓ Word Entropy Analyzer initialized")
    
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
        """
        Calculate how evenly a word is distributed across stories
        
        Higher = word appears in many stories evenly
        Lower = word appears concentrated in few stories
        
        Args:
            word: Word to analyze
            period_items: Items in this period
            
        Returns:
            Distribution entropy
        """
        # Count how many stories contain this word
        story_contains_word = []
        
        for item in period_items:
            words = item.get('words', '').split('|') if item.get('words') else []
            contains = 1 if word in words else 0
            story_contains_word.append(contains)
        
        if not story_contains_word or sum(story_contains_word) == 0:
            return 0.0
        
        # Calculate probability of word appearing
        prob_appears = sum(story_contains_word) / len(story_contains_word)
        prob_not_appears = 1 - prob_appears
        
        # Binary entropy
        entropy = 0.0
        if prob_appears > 0:
            entropy -= prob_appears * math.log2(prob_appears)
        if prob_not_appears > 0:
            entropy -= prob_not_appears * math.log2(prob_not_appears)
        
        return entropy
    
    def calculate_temporal_entropy(self, word: str, periods: List[Dict]) -> float:
        """
        Calculate how unpredictable a word's frequency is over time
        
        Higher = word usage varies unpredictably across periods
        Lower = word usage is consistent/predictable
        
        Args:
            word: Word to analyze
            periods: All time periods
            
        Returns:
            Temporal entropy
        """
        # Get word counts per period
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
        
        # Calculate entropy of distribution across periods
        entropy = 0.0
        for count in counts:
            if count > 0:
                prob = count / total
                entropy -= prob * math.log2(prob)
        
        return entropy
    
    def calculate_context_entropy(self, word: str, period_items: List[Dict]) -> float:
        """
        Calculate diversity of contexts where word appears
        
        Looks at words that appear near this word (context window)
        Higher = appears in many different contexts
        Lower = appears in similar contexts
        
        Args:
            word: Word to analyze
            period_items: Items in this period
            
        Returns:
            Context entropy
        """
        context_words = []
        
        for item in period_items:
            words = item.get('words', '').split('|') if item.get('words') else []
            
            # Find positions of target word
            for i, w in enumerate(words):
                if w == word:
                    # Get context window (2 words before and after)
                    start = max(0, i - 2)
                    end = min(len(words), i + 3)
                    
                    context = words[start:i] + words[i+1:end]
                    context_words.extend(context)
        
        if not context_words:
            return 0.0
        
        # Calculate entropy of context words
        word_counts = Counter(context_words)
        total = len(context_words)
        
        entropy = 0.0
        for count in word_counts.values():
            if count > 0:
                prob = count / total
                entropy -= prob * math.log2(prob)
        
        return entropy
    
    def calculate_cooccurrence_entropy(self, word: str, period_items: List[Dict]) -> float:
        """
        Calculate unpredictability of words that co-occur with this word
        
        Higher = appears with many different words
        Lower = appears with same words repeatedly
        
        Args:
            word: Word to analyze
            period_items: Items in this period
            
        Returns:
            Co-occurrence entropy
        """
        cooccurring_words = []
        
        for item in period_items:
            words = item.get('words', '').split('|') if item.get('words') else []
            
            if word in words:
                # All other words in this story co-occur with target word
                other_words = [w for w in words if w != word]
                cooccurring_words.extend(other_words)
        
        if not cooccurring_words:
            return 0.0
        
        # Calculate entropy
        word_counts = Counter(cooccurring_words)
        total = len(cooccurring_words)
        
        entropy = 0.0
        for count in word_counts.values():
            if count > 0:
                prob = count / total
                entropy -= prob * math.log2(prob)
        
        return entropy
    
    def analyze_word(self, word: str, periods: List[Dict]) -> Dict:
        """
        Calculate all entropy metrics for a word
        
        Args:
            word: Word to analyze
            periods: All periods
            
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
        monitoring_period = periods[1]  # Middle period
        
        metrics = {
            'word': word,
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
        """Run complete word-level entropy analysis"""
        print("\n" + "=" * 120)
        print("WORD-LEVEL ENTROPY ANALYSIS")
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
        
        # Get all unique words from monitoring period
        print("\nExtracting unique words from monitoring period...")
        monitoring_words = []
        for item in periods[1]['items']:
            words = item.get('words', '').split('|') if item.get('words') else []
            monitoring_words.extend(words)
        
        word_counts = Counter(monitoring_words)
        
        # Filter by minimum frequency
        min_freq = self.config['parameters'].get('min_word_freq', 5)
        significant_words = [word for word, count in word_counts.items() if count >= min_freq]
        
        print(f"Found {len(significant_words)} words with frequency >= {min_freq}")
        
        # Analyze each word
        print("\nCalculating entropy metrics for each word...")
        word_metrics = []
        
        total = len(significant_words)
        for i, word in enumerate(significant_words, 1):
            if i % 50 == 0:
                progress = (i / total) * 100
                print(f"  Progress: {progress:.1f}% ({i}/{total})")
            
            metrics = self.analyze_word(word, periods)
            word_metrics.append(metrics)
        
        # Sort by z-score
        word_metrics.sort(key=lambda x: x['max_z_score'], reverse=True)
        
        # Display results
        self.display_results(word_metrics)
        
        # Save to CSV
        self.save_results(word_metrics)
    
    def display_results(self, word_metrics: List[Dict]):
        """Display word entropy results"""
        print("\n" + "=" * 120)
        print("TOP WORDS BY Z-SCORE WITH ENTROPY METRICS")
        print("=" * 120)
        print("\nEntropy Metric Descriptions:")
        print("  Distribution:   How evenly word appears across stories (0-1)")
        print("  Temporal:       How unpredictable word frequency is over time")
        print("  Context:        Diversity of surrounding words")
        print("  Co-occurrence:  Variety of words appearing with this word")
        print()
        
        # Header
        header = f"{'Word':<20}{'Base':>8}{'Mon':>8}{'Event':>8}{'Z-Score':>10}{'Vel':>6}"
        header += f"{'Dist':>8}{'Temp':>8}{'Ctx':>8}{'CoOc':>8}{'Signal':>12}"
        print(header)
        print("-" * 120)
        
        # Top words
        top_n = self.config['parameters'].get('top_n_words', 40)
        for metrics in word_metrics[:top_n]:
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
        output_file = os.path.join(output_dir, f"{output_prefix}_word_entropy.csv")
        
        os.makedirs(output_dir, exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'word', 'baseline_count', 'monitoring_count', 'event_count',
                'max_z_score', 'velocity', 'acceleration',
                'distribution_entropy', 'temporal_entropy', 'context_entropy',
                'cooccurrence_entropy', 'signal_strength'
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(word_metrics)
        
        print(f"\n✅ Word entropy analysis saved to: {output_file}")


# Main execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python3 -m src.analyzers.word_entropy <config_file.yaml>")
        print("\nExample: python3 -m src.analyzers.word_entropy configs/experiments/nov2024_test.yaml")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    analyzer = WordEntropyAnalyzer(config_path)
    analyzer.run_analysis()