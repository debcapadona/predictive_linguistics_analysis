"""
numeric_language.py - Analyze numeric language patterns

Track exact numbers and ordinals as predictive signals
Written-out numbers may indicate emphasis, scale, or sequencing
"""

import csv
import os
import yaml
from collections import Counter, defaultdict
from datetime import datetime
from typing import List, Dict, Set
import sys

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.period_manager import PeriodManager
from src.core.stats_calculator import StatsCalculator


class NumericLanguageAnalyzer:
    """Analyze numeric language patterns across periods"""
    
    # Define numeric terms to track
    EXACT_NUMBERS = {
        'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten',
        'eleven', 'twelve', 'thirteen', 'fourteen', 'fifteen', 'sixteen', 'seventeen',
        'eighteen', 'nineteen', 'twenty', 'thirty', 'forty', 'fifty', 'sixty',
        'seventy', 'eighty', 'ninety', 'hundred', 'thousand', 'million', 'billion', 'trillion'
    }
    
    ORDINALS = {
        'first', 'second', 'third', 'fourth', 'fifth', 'sixth', 'seventh', 'eighth',
        'ninth', 'tenth', 'eleventh', 'twelfth', 'thirteenth', 'fourteenth', 'fifteenth',
        'sixteenth', 'seventeenth', 'eighteenth', 'nineteenth', 'twentieth', 'thirtieth'
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
        
        self.all_numeric_terms = self.EXACT_NUMBERS | self.ORDINALS
        print(f"✓ Numeric Language Analyzer initialized")
        print(f"  Tracking {len(self.EXACT_NUMBERS)} exact numbers")
        print(f"  Tracking {len(self.ORDINALS)} ordinals")
    
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
    
    def analyze_period(self, period: Dict) -> Dict[str, any]:
        """
        Analyze numeric language in a period
        
        Args:
            period: Period dictionary with items
            
        Returns:
            Dictionary with numeric term counts and stories
        """
        numeric_counts = Counter()
        numeric_stories = defaultdict(list)
        
        for item in period['items']:
            words = item.get('words', '').split('|') if item.get('words') else []
            words_set = set(words)
            
            # Check for numeric terms
            found_terms = words_set & self.all_numeric_terms
            
            for term in found_terms:
                numeric_counts[term] += words.count(term)
                numeric_stories[term].append({
                    'title': item.get('original_title', ''),
                    'date': item.get('created_at', ''),
                    'count': words.count(term)
                })
        
        return {
            'counts': numeric_counts,
            'stories': numeric_stories,
            'total_numeric_words': sum(numeric_counts.values()),
            'unique_numeric_terms': len(numeric_counts)
        }
    
    def calculate_term_statistics(self, term: str, periods: List[Dict], 
                                  period_analyses: List[Dict]) -> Dict:
        """
        Calculate statistics for a numeric term
        
        Args:
            term: Numeric term
            periods: Period objects
            period_analyses: Analysis results per period
            
        Returns:
            Statistics dictionary
        """
        counts = [analysis['counts'].get(term, 0) for analysis in period_analyses]
        
        # Calculate z-scores and velocity
        stats = StatsCalculator.calculate_full_stats(counts, baseline_periods=1)
        
        # Categorize term
        if term in self.ORDINALS:
            category = 'ordinal'
        elif term in {'million', 'billion', 'trillion'}:
            category = 'scale'
        elif term in {'hundred', 'thousand'}:
            category = 'large'
        else:
            category = 'basic'
        
        return {
            'term': term,
            'category': category,
            'baseline_count': counts[0],
            'monitoring_count': counts[1],
            'event_count': counts[2],
            'total_count': sum(counts),
            'max_z_score': stats['max_z_score'],
            'velocity': stats['velocity'],
            'acceleration': stats['acceleration'],
            'signal_strength': StatsCalculator.get_signal_strength(stats['max_z_score'])
        }
    
    def run_analysis(self):
        """Run complete numeric language analysis"""
        print("\n" + "=" * 120)
        print("NUMERIC LANGUAGE ANALYSIS")
        print("=" * 120)
        
        # Load data
        input_file = self.config['files']['input']
        items = self.load_processed_data(input_file)
        
        # Create periods
        period_definitions = PeriodManager.create_periods_from_config(self.config)
        periods = PeriodManager.create_period_objects(items, period_definitions)
        
        print("\nAnalyzing numeric language per period...")
        
        # Analyze each period
        period_analyses = []
        for period in periods:
            print(f"  {period['label']}...")
            analysis = self.analyze_period(period)
            period_analyses.append(analysis)
            print(f"    Total numeric words: {analysis['total_numeric_words']}")
            print(f"    Unique numeric terms: {analysis['unique_numeric_terms']}")
        
        # Get all terms that appeared
        all_terms = set()
        for analysis in period_analyses:
            all_terms.update(analysis['counts'].keys())
        
        print(f"\nFound {len(all_terms)} numeric terms across all periods")
        
        # Calculate statistics for each term
        print("\nCalculating statistics...")
        term_stats = []
        for term in all_terms:
            stats = self.calculate_term_statistics(term, periods, period_analyses)
            term_stats.append(stats)
        
        # Sort by z-score
        term_stats.sort(key=lambda x: x['max_z_score'], reverse=True)
        
        # Display results
        self.display_results(term_stats, periods, period_analyses)
        
        # Save results
        self.save_results(term_stats, period_analyses, periods)
    
    def display_results(self, term_stats: List[Dict], periods: List[Dict],
                       period_analyses: List[Dict]):
        """Display numeric language analysis results"""
        print("\n" + "=" * 120)
        print("NUMERIC LANGUAGE PATTERNS")
        print("=" * 120)
        
        # Summary by category
        categories = defaultdict(list)
        for stats in term_stats:
            categories[stats['category']].append(stats)
        
        print("\nSUMMARY BY CATEGORY:")
        print("-" * 120)
        for category in ['scale', 'large', 'ordinal', 'basic']:
            if category in categories:
                terms = categories[category]
                total = sum(s['total_count'] for s in terms)
                print(f"  {category.upper():12s}: {len(terms):3d} terms, {total:5d} total occurrences")
        
        # Top terms by z-score
        print("\n" + "=" * 120)
        print("TOP NUMERIC TERMS BY Z-SCORE")
        print("=" * 120)
        
        header = f"{'Term':<20}{'Category':<12}{'Baseline':>10}{'Monitoring':>12}{'Event':>10}"
        header += f"{'Z-Score':>10}{'Velocity':>10}{'Signal':>15}"
        print(header)
        print("-" * 120)
        
        top_n = min(30, len(term_stats))
        for stats in term_stats[:top_n]:
            row = f"{stats['term']:<20}{stats['category']:<12}"
            row += f"{stats['baseline_count']:>10}{stats['monitoring_count']:>12}{stats['event_count']:>10}"
            row += f"{stats['max_z_score']:>10.2f}{stats['velocity']:>10}"
            
            emoji = StatsCalculator.get_signal_emoji(stats['max_z_score'])
            row += f"{emoji} {stats['signal_strength']:>12}"
            
            print(row)
        
        print("=" * 120)
        
        # Show period totals
        print("\nPERIOD TOTALS:")
        print("-" * 120)
        for period, analysis in zip(periods, period_analyses):
            print(f"{period['label']:20s}: {analysis['total_numeric_words']:5d} numeric words "
                  f"({analysis['unique_numeric_terms']:3d} unique terms)")
        
        print("=" * 120)
    
    def save_results(self, term_stats: List[Dict], period_analyses: List[Dict],
                    periods: List[Dict]):
        """Save results to CSV"""
        output_dir = self.config['files']['output_dir']
        output_prefix = self.config['files']['output_prefix']
        
        # Save term statistics
        term_file = os.path.join(output_dir, f"{output_prefix}_numeric_terms.csv")
        os.makedirs(output_dir, exist_ok=True)
        
        with open(term_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'term', 'category', 'baseline_count', 'monitoring_count', 'event_count',
                'total_count', 'max_z_score', 'velocity', 'acceleration', 'signal_strength'
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(term_stats)
        
        print(f"\n✅ Numeric term analysis saved to: {term_file}")
        
        # Save period summary
        summary_file = os.path.join(output_dir, f"{output_prefix}_numeric_summary.csv")
        
        with open(summary_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['period', 'start_date', 'end_date', 'total_numeric_words', 'unique_numeric_terms']
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for period, analysis in zip(periods, period_analyses):
                row = {
                    'period': period['label'],
                    'start_date': period['start_date'].strftime('%Y-%m-%d'),
                    'end_date': period['end_date'].strftime('%Y-%m-%d'),
                    'total_numeric_words': analysis['total_numeric_words'],
                    'unique_numeric_terms': analysis['unique_numeric_terms']
                }
                writer.writerow(row)
        
        print(f"✅ Numeric summary saved to: {summary_file}")


# Main execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python3 -m src.analyzers.numeric_language <config_file.yaml>")
        print("\nExample: python3 -m src.analyzers.numeric_language configs/experiments/may_july_2024.yaml")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    analyzer = NumericLanguageAnalyzer(config_path)
    analyzer.run_analysis()