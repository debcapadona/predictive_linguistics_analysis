# frequency_analyzer.py - Advanced temporal frequency analysis with z-scores
# Detects statistically significant linguistic changes

import csv
from collections import Counter, defaultdict
from datetime import datetime, timedelta
import statistics
import math

class FrequencyAnalyzer:
    """
    Analyzes word frequency changes over time with statistical significance
    """
    
    def __init__(self):
        """Initialize the analyzer"""
        print("Advanced Frequency Analyzer initialized")
    
    def load_processed_data(self, filename):
        """Load processed data from CSV"""
        print(f"Loading data from: {filename}")
        
        stories = []
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row['date'] = datetime.strptime(row['created_at'], '%Y-%m-%d %H:%M:%S')
                stories.append(row)
        
        print(f"Loaded {len(stories)} stories")
        return stories
    
    def create_time_periods(self, stories, days_total):
        """Divide stories into time periods"""
        stories.sort(key=lambda x: x['date'])
        
        start_date = stories[0]['date']
        end_date = stories[-1]['date']
        
        print(f"\nDate range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Determine period sizes
        if days_total <= 30:
            period_configs = [
                (1, 10, "Days 1-10"),
                (11, 20, "Days 11-20"),
                (21, 30, "Days 21-30")
            ]
        elif days_total <= 60:
            period_configs = [
                (1, 20, "Days 1-20"),
                (21, 40, "Days 21-40"),
                (41, 60, "Days 41-60")
            ]
        else:
            period_configs = [
                (1, 30, "Days 1-30"),
                (31, 60, "Days 31-60"),
                (61, 90, "Days 61-90")
            ]
        
        periods = []
        
        for start_day, end_day, label in period_configs:
            period_start = start_date + timedelta(days=start_day-1)
            period_end = start_date + timedelta(days=end_day)
            
            period_stories = [
                s for s in stories 
                if period_start <= s['date'] < period_end
            ]
            
            periods.append({
                'label': label,
                'start_day': start_day,
                'end_day': end_day,
                'start_date': period_start,
                'end_date': period_end,
                'stories': period_stories,
                'story_count': len(period_stories)
            })
        
        return periods
    
    def count_words_in_period(self, period):
        """Count word frequencies for a time period"""
        all_words = []
        
        for story in period['stories']:
            words = story['words'].split('|') if story['words'] else []
            all_words.extend(words)
        
        return Counter(all_words)
    
    def calculate_baseline_stats(self, period_counts, word):
        """
        Calculate baseline statistics for a word
        
        Args:
            period_counts: List of Counter objects for each period
            word: Word to analyze
            
        Returns:
            Dictionary with mean, stdev, z_score
        """
        # Get counts for this word in each period
        counts = [pc.get(word, 0) for pc in period_counts]
        
        # Calculate baseline (use first periods)
        baseline_periods = counts[:-1]  # All but last period
        
        if not baseline_periods or all(c == 0 for c in baseline_periods):
            return {
                'mean': 0,
                'stdev': 0,
                'z_score': 0,
                'current': counts[-1],
                'baseline_max': 0
            }
        
        mean = statistics.mean(baseline_periods)
        
        # Calculate standard deviation
        if len(baseline_periods) > 1:
            stdev = statistics.stdev(baseline_periods)
        else:
            stdev = 0
        
        # Z-score for current period
        current = counts[-1]
        
        if stdev > 0:
            z_score = (current - mean) / stdev
        else:
            # If stdev is 0, check if there's a change
            z_score = 10.0 if current > mean else 0.0
        
        return {
            'mean': mean,
            'stdev': stdev,
            'z_score': z_score,
            'current': current,
            'baseline_max': max(baseline_periods) if baseline_periods else 0,
            'velocity': current - baseline_periods[-1] if baseline_periods else 0
        }
    
    def calculate_velocity_acceleration(self, counts):
        """
        Calculate velocity (rate of change) and acceleration
        
        Args:
            counts: List of counts for each period
            
        Returns:
            Dictionary with velocity and acceleration
        """
        if len(counts) < 2:
            return {'velocity': 0, 'acceleration': 0}
        
        # Velocity: change from period to period
        velocities = []
        for i in range(1, len(counts)):
            velocity = counts[i] - counts[i-1]
            velocities.append(velocity)
        
        # Acceleration: change in velocity
        if len(velocities) >= 2:
            acceleration = velocities[-1] - velocities[-2]
        else:
            acceleration = 0
        
        return {
            'velocity': velocities[-1] if velocities else 0,
            'acceleration': acceleration
        }
    
    def create_significance_table(self, periods, min_z_score=2.0, top_n=30):
        """
        Create table showing only statistically significant changes
        
        Args:
            periods: List of period dictionaries
            min_z_score: Minimum z-score to include (2.0 = 95% confidence)
            top_n: Number of top results to show
        """
        print("\n" + "=" * 120)
        print("STATISTICALLY SIGNIFICANT WORD CHANGES")
        print(f"(Z-Score > {min_z_score} = 95%+ confidence this is a real change, not random)")
        print("=" * 120)
        
        # Get word counts for each period
        period_counts = []
        all_words = set()
        
        for period in periods:
            word_freq = self.count_words_in_period(period)
            period_counts.append(word_freq)
            all_words.update(word_freq.keys())
        
        # Calculate statistics for each word
        word_stats = []
        
        for word in all_words:
            if not word:
                continue
            
            stats = self.calculate_baseline_stats(period_counts, word)
            counts = [pc.get(word, 0) for pc in period_counts]
            vel_acc = self.calculate_velocity_acceleration(counts)
            
            # Only include if z-score meets threshold
            if stats['z_score'] >= min_z_score:
                word_stats.append({
                    'word': word,
                    'counts': counts,
                    'baseline_mean': stats['mean'],
                    'current': stats['current'],
                    'z_score': stats['z_score'],
                    'velocity': vel_acc['velocity'],
                    'acceleration': vel_acc['acceleration'],
                    'total': sum(counts)
                })
        
        # Sort by z-score (highest first)
        word_stats.sort(key=lambda x: x['z_score'], reverse=True)
        
        # Print period info
        print("\nPeriod Information:")
        for i, period in enumerate(periods):
            print(f"  Period {i+1}: {period['label']} ({period['story_count']} stories)")
        
        print("\n" + "-" * 120)
        
        # Table header
        header = f"{'Word':<20}"
        for i in range(len(periods)):
            header += f"P{i+1:>6}"
        header += f"{'Z-Score':>10}{'Velocity':>10}{'Accel':>8}{'Signal':>10}"
        
        print(header)
        print("-" * 120)
        
        # Print top words
        for item in word_stats[:top_n]:
            row = f"{item['word']:<20}"
            
            for count in item['counts']:
                row += f"{count:>7}"
            
            row += f"{item['z_score']:>10.2f}"
            row += f"{item['velocity']:>10}"
            row += f"{item['acceleration']:>8}"
            
            # Signal strength
            if item['z_score'] > 3.0:
                signal = "🔥 STRONG"
            elif item['z_score'] > 2.5:
                signal = "⚡ HIGH"
            else:
                signal = "✓ Medium"
            
            row += f"{signal:>10}"
            
            print(row)
        
        print("-" * 120)
        print("\nLegend:")
        print("  Z-Score: Statistical significance (>2.0 = real, >3.0 = very strong)")
        print("  Velocity: Change from previous period (positive = growing)")
        print("  Accel: Change in velocity (positive = accelerating growth)")
        print("  🔥 STRONG = Z > 3.0 (99%+ confidence)")
        print("  ⚡ HIGH   = Z > 2.5 (98%+ confidence)")
        print("  ✓ Medium = Z > 2.0 (95%+ confidence)")
        
        print("=" * 120)
        
        return word_stats
    
    def show_emerging_terms(self, word_stats, top_n=15):
        """
        Show terms that are emerging (low baseline, high current spike)
        
        Args:
            word_stats: List of word statistics
            top_n: Number to show
        """
        print("\n" + "=" * 100)
        print("🚀 EMERGING TERMS (New or suddenly spiking)")
        print("=" * 100)
        
        # Filter for emerging: low baseline mean, high z-score
        emerging = [
            w for w in word_stats 
            if w['baseline_mean'] < 5 and w['z_score'] > 2.5
        ]
        
        # Sort by z-score
        emerging.sort(key=lambda x: x['z_score'], reverse=True)
        
        print("\n" + "-" * 100)
        print(f"{'Word':<20}{'Baseline Avg':>15}{'Current':>10}{'Z-Score':>12}{'Growth':>15}")
        print("-" * 100)
        
        for item in emerging[:top_n]:
            baseline_avg = item['baseline_mean']
            current = item['current']
            
            if baseline_avg > 0:
                growth_pct = ((current - baseline_avg) / baseline_avg) * 100
                growth_str = f"+{growth_pct:.0f}%"
            else:
                growth_str = "NEW"
            
            print(f"{item['word']:<20}{baseline_avg:>15.1f}{current:>10}{item['z_score']:>12.2f}{growth_str:>15}")
        
        print("=" * 100)
        
        if emerging:
            print("\n💡 These words are appearing much more frequently than their historical baseline.")
            print("   They may indicate emerging trends or upcoming events.")
        else:
            print("\n   No strong emerging terms detected.")

# Main execution
if __name__ == "__main__":
    print("=" * 120)
    print("ADVANCED FREQUENCY ANALYZER - STATISTICAL SIGNIFICANCE TESTING")
    print("=" * 120)
    
    analyzer = FrequencyAnalyzer()
    
    # Load processed data
    stories = analyzer.load_processed_data('data/processed/hackernews_processed.csv')
    
    if stories:
        stories.sort(key=lambda x: x['date'])
        days_total = (stories[-1]['date'] - stories[0]['date']).days + 1
        print(f"Total days of data: {days_total}")
        
        # Create time periods
        periods = analyzer.create_time_periods(stories, days_total)
        
        # Analyze with statistical significance
        word_stats = analyzer.create_significance_table(
            periods, 
            min_z_score=2.0,  # 95% confidence threshold
            top_n=30
        )
        
        # Show emerging terms
        analyzer.show_emerging_terms(word_stats, top_n=15)
    
    print("\n" + "=" * 120)
    print("Done! Now you can see REAL signals vs noise.")
    print("=" * 120)