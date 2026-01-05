"""
entropy.py - Entropy analysis for predictive linguistics

Calculates multiple entropy metrics to detect information uncertainty changes
Tracks: Shannon, Vocabulary, Topic, Sentiment, Compression, N-gram diversity
"""

import csv
import math
import gzip
import os
import yaml
from collections import Counter
from datetime import datetime
from typing import List, Dict, Tuple
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.period_manager import PeriodManager
from src.core.stats_calculator import StatsCalculator
from src.core.text_processor import TextProcessor

# For sentiment entropy
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class EntropyAnalyzer:
    """Analyze multiple types of entropy across time periods"""
    
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
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
        
        print("✓ Entropy Analyzer initialized")
    
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
    
    def calculate_shannon_entropy(self, items: List[Dict]) -> float:
        """
        Calculate Shannon entropy of word distribution
        H(X) = -Σ p(x) log₂ p(x)
        
        Higher values = more diverse/uncertain vocabulary
        
        Args:
            items: List of processed items
            
        Returns:
            Shannon entropy value
        """
        # Get all words
        all_words = []
        for item in items:
            words = item.get('words', '').split('|') if item.get('words') else []
            all_words.extend(words)
        
        if not all_words:
            return 0.0
        
        # Count frequencies
        word_counts = Counter(all_words)
        total = len(all_words)
        
        # Calculate entropy
        entropy = 0.0
        for count in word_counts.values():
            if count > 0:
                probability = count / total
                entropy -= probability * math.log2(probability)
        
        return entropy
    
    def calculate_vocabulary_entropy(self, items: List[Dict]) -> float:
        """
        Calculate vocabulary diversity (Type-Token Ratio)
        
        TTR = unique words / total words
        Higher = more diverse vocabulary
        
        Args:
            items: List of processed items
            
        Returns:
            Vocabulary entropy (TTR)
        """
        all_words = []
        for item in items:
            words = item.get('words', '').split('|') if item.get('words') else []
            all_words.extend(words)
        
        if not all_words:
            return 0.0
        
        unique_words = len(set(all_words))
        total_words = len(all_words)
        
        return unique_words / total_words if total_words > 0 else 0.0
    
    def calculate_sentiment_entropy(self, items: List[Dict]) -> float:
        """
        Calculate sentiment variance/entropy
        
        Measures emotional uncertainty in the discourse
        Higher = more varied/conflicted sentiment
        
        Args:
            items: List of processed items
            
        Returns:
            Sentiment entropy (variance)
        """
        sentiments = []
        
        for item in items:
            text = item.get('original_title', '')
            if text:
                # Use VADER for compound sentiment score
                sentiment = self.sentiment_analyzer.polarity_scores(text)
                sentiments.append(sentiment['compound'])
        
        if not sentiments:
            return 0.0
        
        # Calculate variance as entropy measure
        mean_sentiment = sum(sentiments) / len(sentiments)
        variance = sum((s - mean_sentiment) ** 2 for s in sentiments) / len(sentiments)
        
        return variance
    
    def calculate_compression_ratio(self, items: List[Dict]) -> float:
        """
        Calculate text compression ratio
        
        Higher compression = lower entropy (more predictable)
        So we return 1 - compression_ratio for consistency
        (higher value = higher entropy)
        
        Args:
            items: List of processed items
            
        Returns:
            Normalized compression entropy
        """
        # Concatenate all text
        all_text = ' '.join([
            item.get('original_title', '') 
            for item in items 
            if item.get('original_title')
        ])
        
        if not all_text:
            return 0.0
        
        # Compress and measure
        original_size = len(all_text.encode('utf-8'))
        compressed_size = len(gzip.compress(all_text.encode('utf-8')))
        
        compression_ratio = compressed_size / original_size if original_size > 0 else 1.0
        
        # Return entropy measure (higher = less compressible = higher entropy)
        return 1.0 - compression_ratio
    
    def calculate_ngram_diversity(self, items: List[Dict]) -> float:
        """
        Calculate n-gram diversity
        
        Unique bigrams / Total bigrams
        Higher = more diverse phrase usage
        
        Args:
            items: List of processed items
            
        Returns:
            N-gram diversity ratio
        """
        all_bigrams = []
        for item in items:
            bigrams = item.get('bigrams', '').split('|') if item.get('bigrams') else []
            all_bigrams.extend(bigrams)
        
        if not all_bigrams:
            return 0.0
        
        unique_bigrams = len(set(all_bigrams))
        total_bigrams = len(all_bigrams)
        
        return unique_bigrams / total_bigrams if total_bigrams > 0 else 0.0
    
    def calculate_perplexity_proxy(self, items: List[Dict]) -> float:
        """
        Calculate a proxy for perplexity using word frequency distribution
        
        True perplexity requires a language model, but we can estimate
        using the inverse of average word probability
        
        Lower perplexity = more predictable
        We return it as-is (higher = less predictable = higher entropy)
        
        Args:
            items: List of processed items
            
        Returns:
            Perplexity proxy value
        """
        all_words = []
        for item in items:
            words = item.get('words', '').split('|') if item.get('words') else []
            all_words.extend(words)
        
        if not all_words:
            return 0.0
        
        # Calculate word probabilities
        word_counts = Counter(all_words)
        total = len(all_words)
        
        # Calculate cross-entropy
        cross_entropy = 0.0
        for word in all_words:
            probability = word_counts[word] / total
            if probability > 0:
                cross_entropy -= math.log2(probability)
        
        cross_entropy /= len(all_words)
        
        # Perplexity = 2^(cross-entropy)
        perplexity = 2 ** cross_entropy
        
        return perplexity
    
    def analyze_period(self, period: Dict) -> Dict[str, float]:
        """
        Calculate all entropy metrics for a period
        
        Args:
            period: Period dictionary with items
            
        Returns:
            Dictionary with all entropy metrics
        """
        items = period['items']
        
        metrics = {
            'shannon_entropy': self.calculate_shannon_entropy(items),
            'vocabulary_entropy': self.calculate_vocabulary_entropy(items),
            'sentiment_entropy': self.calculate_sentiment_entropy(items),
            'compression_entropy': self.calculate_compression_ratio(items),
            'ngram_diversity': self.calculate_ngram_diversity(items),
            'perplexity': self.calculate_perplexity_proxy(items)
        }
        
        return metrics
    
    def run_analysis(self):
        """Run complete entropy analysis"""
        print("\n" + "=" * 120)
        print("ENTROPY ANALYSIS - INFORMATION UNCERTAINTY TRACKING")
        print("=" * 120)
        
        # Load data
        input_file = self.config['files']['input']
        items = self.load_processed_data(input_file)
        
        # Create periods
        period_definitions = PeriodManager.create_periods_from_config(self.config)
        periods = PeriodManager.create_period_objects(items, period_definitions)
        
        print("\nAnalyzing entropy across periods...")
        
        # Calculate entropy for each period
        period_metrics = []
        for period in periods:
            print(f"  Calculating entropy for {period['label']}...")
            metrics = self.analyze_period(period)
            period_metrics.append(metrics)
        
        # Display results
        self.display_results(periods, period_metrics)
        
        # Calculate z-scores for entropy changes
        self.calculate_entropy_signals(periods, period_metrics)
        
        # Save to CSV
        self.save_results(periods, period_metrics)
    
    def display_results(self, periods: List[Dict], period_metrics: List[Dict]):
        """Display entropy metrics table"""
        print("\n" + "=" * 120)
        print("ENTROPY METRICS BY PERIOD")
        print("=" * 120)
        print("\nMetric Descriptions:")
        print("  Shannon Entropy:      Information diversity (higher = more uncertain vocabulary)")
        print("  Vocabulary Entropy:   Type-Token Ratio (higher = more unique words)")
        print("  Sentiment Entropy:    Emotional variance (higher = more conflicted sentiment)")
        print("  Compression Entropy:  Text unpredictability (higher = less compressible)")
        print("  N-gram Diversity:     Phrase uniqueness (higher = more diverse phrases)")
        print("  Perplexity:          Language unpredictability (higher = more surprising)")
        print()
        
        # Table header
        header = f"{'Period':<25}"
        metrics_names = ['Shannon', 'Vocabulary', 'Sentiment', 'Compression', 'N-gram', 'Perplexity']
        for name in metrics_names:
            header += f"{name:>12}"
        
        print(header)
        print("-" * 120)
        
        # Data rows
        for period, metrics in zip(periods, period_metrics):
            row = f"{period['label']:<25}"
            row += f"{metrics['shannon_entropy']:>12.3f}"
            row += f"{metrics['vocabulary_entropy']:>12.3f}"
            row += f"{metrics['sentiment_entropy']:>12.3f}"
            row += f"{metrics['compression_entropy']:>12.3f}"
            row += f"{metrics['ngram_diversity']:>12.3f}"
            row += f"{metrics['perplexity']:>12.1f}"
            print(row)
        
        print("=" * 120)
    
    def calculate_entropy_signals(self, periods: List[Dict], period_metrics: List[Dict]):
        """Calculate z-scores for entropy changes"""
        print("\n" + "=" * 120)
        print("ENTROPY CHANGE SIGNALS (Z-Scores)")
        print("=" * 120)
        
        metric_names = {
            'shannon_entropy': 'Shannon',
            'vocabulary_entropy': 'Vocabulary',
            'sentiment_entropy': 'Sentiment',
            'compression_entropy': 'Compression',
            'ngram_diversity': 'N-gram',
            'perplexity': 'Perplexity'
        }
        
        # Calculate z-scores for each metric
        print(f"\n{'Metric':<20}{'Baseline':>12}{'Monitoring':>12}{'Event':>12}{'Z-Score':>12}{'Signal':>15}")
        print("-" * 120)
        
        for metric_key, metric_name in metric_names.items():
            values = [m[metric_key] for m in period_metrics]
            stats = StatsCalculator.calculate_full_stats(values, baseline_periods=1)
            
            z_score = stats['max_z_score']
            signal = StatsCalculator.get_signal_strength(z_score)
            emoji = StatsCalculator.get_signal_emoji(z_score)
            
            row = f"{metric_name:<20}"
            for val in values:
                row += f"{val:>12.3f}"
            row += f"{z_score:>12.2f}"
            row += f"{emoji} {signal:>12}"
            
            print(row)
        
        print("=" * 120)
    
    def save_results(self, periods: List[Dict], period_metrics: List[Dict]):
        """Save results to CSV"""
        output_dir = self.config['files']['output_dir']
        output_prefix = self.config['files']['output_prefix']
        output_file = os.path.join(output_dir, f"{output_prefix}_entropy.csv")
        
        os.makedirs(output_dir, exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'period', 'start_date', 'end_date', 'item_count',
                'shannon_entropy', 'vocabulary_entropy', 'sentiment_entropy',
                'compression_entropy', 'ngram_diversity', 'perplexity'
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for period, metrics in zip(periods, period_metrics):
                row = {
                    'period': period['label'],
                    'start_date': period['start_date'].strftime('%Y-%m-%d'),
                    'end_date': period['end_date'].strftime('%Y-%m-%d'),
                    'item_count': period['item_count'],
                    **metrics
                }
                writer.writerow(row)
        
        print(f"\n✅ Entropy analysis saved to: {output_file}")


# Main execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python3 -m src.analyzers.entropy <config_file.yaml>")
        print("\nExample: python3 -m src.analyzers.entropy configs/experiments/may_june_july.yaml")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    analyzer = EntropyAnalyzer(config_path)
    analyzer.run_analysis()