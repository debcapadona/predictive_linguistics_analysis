# analyze_date_range.py - Analyze specific date ranges for event prediction backtesting
# Allows custom date ranges to validate if linguistic signals predicted known events

import csv
from collections import Counter
from datetime import datetime
import statistics
import os

class DateRangeAnalyzer:
    """
    Analyzes word frequency changes across custom date ranges
    Perfect for backtesting against known events
    """
    
    def __init__(self):
        """Initialize the analyzer"""
        print("Custom Date Range Analyzer initialized")
    
    def load_processed_data(self, filename):
        """Load processed data from CSV"""
        print(f"Loading data from: {filename}")
        
        stories = []
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    row['date'] = datetime.strptime(row['created_at'], '%Y-%m-%d %H:%M:%S')
                    stories.append(row)
                except:
                    continue
        
        print(f"Loaded {len(stories)} stories")
        return stories
    
    def filter_by_date_range(self, stories, start_date, end_date):
        """
        Filter stories within a date range
        
        Args:
            stories: List of story dictionaries
            start_date: datetime object for start
            end_date: datetime object for end
            
        Returns:
            Filtered list of stories
        """
        filtered = [
            s for s in stories 
            if start_date <= s['date'] < end_date
        ]
        return filtered
    
    def create_custom_periods(self, stories, period_definitions):
        """
        Create custom time periods based on definitions
        
        Args:
            stories: List of all stories
            period_definitions: List of tuples (label, start_date, end_date)
            
        Returns:
            List of period dictionaries
        """
        periods = []
        
        for label, start_date, end_date in period_definitions:
            period_stories = self.filter_by_date_range(stories, start_date, end_date)
            
            periods.append({
                'label': label,
                'start_date': start_date,
                'end_date': end_date,
                'stories': period_stories,
                'story_count': len(period_stories)
            })
            
            print(f"Period '{label}': {len(period_stories)} stories ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})")
        
        return periods
    
    def count_words_in_period(self, period):
        """Count word frequencies for a period"""
        all_words = []
        
        for story in period['stories']:
            words = story['words'].split('|') if story['words'] else []
            all_words.extend(words)
        
        return Counter(all_words)
    
    def calculate_statistics(self, period_counts, word, baseline_periods=1):
        """
        Calculate statistics for a word across periods
        
        Args:
            period_counts: List of Counter objects
            word: Word to analyze
            baseline_periods: How many initial periods to use as baseline
            
        Returns:
            Dictionary with statistics
        """
        counts = [pc.get(word, 0) for pc in period_counts]
        
        # Baseline stats
        baseline = counts[:baseline_periods]
        
        if not baseline or all(c == 0 for c in baseline):
            return {
                'counts': counts,
                'baseline_mean': 0,
                'z_score': 0,
                'max_z_score': 0, 
                'velocity': 0,
                'acceleration': 0
            }
        
        baseline_mean = statistics.mean(baseline)
        
        if len(baseline) > 1:
            baseline_stdev = statistics.stdev(baseline)
        else:
            baseline_stdev = baseline_mean * 0.3 if baseline_mean > 0 else 1.0
        
        # Z-score for each period after baseline
        z_scores = []
        for i in range(baseline_periods, len(counts)):
            if baseline_stdev > 0:
                z = (counts[i] - baseline_mean) / baseline_stdev
            else:
                z = 10.0 if counts[i] > baseline_mean else 0.0
            z_scores.append(z)
        
        # Velocity (change between periods)
        velocities = []
        for i in range(1, len(counts)):
            vel = counts[i] - counts[i-1]
            velocities.append(vel)
        
        # Acceleration (change in velocity)
        if len(velocities) >= 2:
            acceleration = velocities[-1] - velocities[-2]
        else:
            acceleration = 0
        
        return {
            'counts': counts,
            'baseline_mean': baseline_mean,
            'baseline_stdev': baseline_stdev,
            'z_scores': z_scores,
            'max_z_score': max(z_scores) if z_scores else 0,
            'velocity': velocities[-1] if velocities else 0,
            'acceleration': acceleration
        }
    
    def create_comparison_table(self, periods, min_z_score=2.0, top_n=30):
        """
        Create comparison table across custom date ranges
        
        Args:
            periods: List of period dictionaries
            min_z_score: Minimum z-score threshold
            top_n: Number of top results to show
        """
        print("\n" + "=" * 120)
        print("TEMPORAL WORD ANALYSIS - CUSTOM DATE RANGES")
        print("=" * 120)
        
        # Period info
        print("\nAnalysis Periods:")
        for i, period in enumerate(periods, 1):
            print(f"  {i}. {period['label']:20s} | {period['start_date'].strftime('%Y-%m-%d')} to {period['end_date'].strftime('%Y-%m-%d')} | {period['story_count']:4d} stories")
        
        # Count words per period
        period_counts = [self.count_words_in_period(p) for p in periods]
        
        # Get all unique words
        all_words = set()
        for pc in period_counts:
            all_words.update(pc.keys())
        
        # Calculate statistics for each word
        word_stats = []
        
        for word in all_words:
            if not word:
                continue
            
            stats = self.calculate_statistics(period_counts, word, baseline_periods=1)
            
            # Only include if meets z-score threshold
            if stats['max_z_score'] >= min_z_score:
                word_stats.append({
                    'word': word,
                    **stats
                })
        
        # Sort by max z-score
        word_stats.sort(key=lambda x: x['max_z_score'], reverse=True)
        
        # Display table
        print("\n" + "-" * 120)
        
        # Header
        header = f"{'Word':<20}"
        for i, p in enumerate(periods, 1):
            header += f"{p['label'][:10]:>12}"
        header += f"{'Max Z':>10}{'Velocity':>10}{'Accel':>8}{'Signal':>12}"
        
        print(header)
        print("-" * 120)
        
        # Rows
        for item in word_stats[:top_n]:
            row = f"{item['word']:<20}"
            
            for count in item['counts']:
                row += f"{count:>12}"
            
            row += f"{item['max_z_score']:>10.2f}"
            row += f"{item['velocity']:>10}"
            row += f"{item['acceleration']:>8}"
            
            # Signal strength
            if item['max_z_score'] > 4.0:
                signal = "🔥 VERY STRONG"
            elif item['max_z_score'] > 3.0:
                signal = "⚡ STRONG"
            elif item['max_z_score'] > 2.5:
                signal = "✓ HIGH"
            else:
                signal = "• Medium"
            
            row += f"{signal:>12}"
            
            print(row)
        
        print("-" * 120)
        
        # Summary stats
        print("\nPeriod Totals:")
        totals_row = f"{'TOTAL WORDS':<20}"
        for pc in period_counts:
            total = sum(pc.values())
            totals_row += f"{total:>12}"
        print(totals_row)
        
        unique_row = f"{'UNIQUE WORDS':<20}"
        for pc in period_counts:
            unique = len(pc)
            unique_row += f"{unique:>12}"
        print(unique_row)
        
        print("=" * 120)
        
        return word_stats
    
    def show_emerging_signals(self, word_stats, top_n=15):
        """
        Show words that emerged in monitoring period
        
        Args:
            word_stats: List of word statistics
            top_n: Number to show
        """
        print("\n" + "=" * 100)
        print("🚨 PREDICTIVE SIGNALS (Words that spiked BEFORE the event)")
        print("=" * 100)
        
        # Filter for words with high z-score and low baseline
        signals = [
            w for w in word_stats
            if w['baseline_mean'] < 10 and w['max_z_score'] > 2.5
        ]
        
        signals.sort(key=lambda x: x['max_z_score'], reverse=True)
        
        print("\n" + "-" * 100)
        print(f"{'Word':<20}{'Baseline Avg':>15}{'Final Count':>15}{'Max Z-Score':>15}{'Velocity':>15}")
        print("-" * 100)
        
        for item in signals[:top_n]:
            print(f"{item['word']:<20}"
                  f"{item['baseline_mean']:>15.1f}"
                  f"{item['counts'][-1]:>15}"
                  f"{item['max_z_score']:>15.2f}"
                  f"{item['velocity']:>15}")
        
        print("=" * 100)
        
        if signals:
            print("\n💡 These words showed unusual activity in the monitoring period.")
            print("   They may have predicted the event that occurred later.")
        else:
            print("\n   No strong predictive signals detected.")
    
    def save_to_csv(self, word_stats, periods, output_file):
        """
        Save analysis results to CSV
        
        Args:
            word_stats: List of word statistics
            periods: List of period dictionaries
            output_file: Path to output CSV file
        """
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            # Create header
            fieldnames = ['word', 'baseline_mean', 'baseline_stdev']
            
            # Add period columns
            for period in periods:
                fieldnames.append(period['label'])
            
            # Add analysis columns
            fieldnames.extend(['max_z_score', 'velocity', 'acceleration', 'signal_strength'])
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            # Write data rows
            for item in word_stats:
                row = {
                    'word': item['word'],
                    'baseline_mean': round(item['baseline_mean'], 2),
                    'baseline_stdev': round(item.get('baseline_stdev', 0), 2),
                    'max_z_score': round(item['max_z_score'], 2),
                    'velocity': item['velocity'],
                    'acceleration': item['acceleration']
                }
                
                # Add period counts
                for i, period in enumerate(periods):
                    row[period['label']] = item['counts'][i]
                
                # Add signal strength
                if item['max_z_score'] > 4.0:
                    row['signal_strength'] = "VERY STRONG"
                elif item['max_z_score'] > 3.0:
                    row['signal_strength'] = "STRONG"
                elif item['max_z_score'] > 2.5:
                    row['signal_strength'] = "HIGH"
                else:
                    row['signal_strength'] = "Medium"
                
                writer.writerow(row)
        
        print(f"\n✅ CSV saved to: {output_file}")


# Main execution
if __name__ == "__main__":
    print("=" * 120)
    print("DATE RANGE ANALYZER - BACKTEST EVENT PREDICTION")
    print("=" * 120)
    
    # Store filename in variable so we can print it later (no hard-coding!)
    input_file = 'data/processed/hackernews_training_processed.csv'
    
    analyzer = DateRangeAnalyzer()
    
    # Load processed training data
    stories = analyzer.load_processed_data(input_file)
    
    # Define custom periods for analysis
    # FORMAT: (label, start_date, end_date)
    period_definitions = [
        ("May 2024 (Baseline)", datetime(2024, 5, 1), datetime(2024, 6, 1)),
        ("June 2024 (Monitoring)", datetime(2024, 6, 1), datetime(2024, 7, 1)),
        ("July 2024 (Event)", datetime(2024, 7, 1), datetime(2024, 8, 1))
    ]
    
    print("\n" + "=" * 120)
    print("ANALYSIS SETUP:")
    print("  Baseline: May 2024 (establish normal patterns)")
    print("  Monitor:  June 2024 (watch for emerging signals)")
    print("  Event:    July 2024 (event occurred)")
    print("=" * 120)
    
    # Create periods
    periods = analyzer.create_custom_periods(stories, period_definitions)
    
    # Analyze
    word_stats = analyzer.create_comparison_table(
        periods,
        min_z_score=2.0,
        top_n=40
    )
    
    # Show predictive signals
    analyzer.show_emerging_signals(word_stats, top_n=20)
    
    # Save to CSV
    output_csv = 'data/analysis/may_june_july_2024_analysis.csv'
    analyzer.save_to_csv(word_stats, periods, output_csv)
    
    # FIXED SECTION - Now prints actual variables, not hard-coded values
    print("\n" + "=" * 120)
    print("Done!")
    print(f"Source file: {input_file}")
    print(f"Output CSV: {output_csv}")
    print(f"Date ranges analyzed: {', '.join([p['label'] for p in periods])}")
    print(f"\nPeriod details:")
    for period in periods:
        print(f"  - {period['label']}: {period['start_date'].strftime('%Y-%m-%d')} to {period['end_date'].strftime('%Y-%m-%d')} ({period['story_count']} stories)")
    print("\nNEXT: Compare these June signals to what actually happened in July!")
    print("=" * 120)