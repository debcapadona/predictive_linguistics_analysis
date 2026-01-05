# temporal_analyzer.py - Detect temporal markers that indicate event timing
# Identifies time-related words and correlates them with topic clusters

import csv
from collections import Counter, defaultdict
from datetime import datetime
import statistics
import os
import re

class TemporalAnalyzer:
    """
    Analyzes temporal markers (time-related words) that indicate event timing
    """
    
    def __init__(self):
        """Initialize the analyzer with temporal marker categories"""
        
        # Define temporal marker categories
        self.temporal_markers = {
            'immediate': [
                'today', 'tomorrow', 'now', 'currently', 'tonight', 'this morning',
                'this afternoon', 'this evening', 'immediate', 'right now'
            ],
            'short_term': [
                'soon', 'shortly', 'upcoming', 'imminent', 'approaching', 'nearing',
                'this week', 'next week', 'coming days', 'coming week', 'days away',
                'within days', 'any day', 'next few days'
            ],
            'medium_term': [
                'next month', 'this month', 'coming month', 'coming weeks', 'next quarter',
                'within weeks', 'within month', 'few weeks', 'several weeks', 'couple weeks'
            ],
            'long_term': [
                'next year', 'this year', 'coming year', 'coming months', 'next quarter',
                'within months', 'several months', 'long term', 'future', 'eventually'
            ],
            'urgency': [
                'urgent', 'critical', 'emergency', 'breaking', 'alert', 'warning',
                'imminent', 'crisis', 'immediate', 'now', 'asap'
            ]
        }
        
        # Flatten all markers for easy lookup
        self.all_markers = set()
        for category in self.temporal_markers.values():
            self.all_markers.update(category)
        
        print(f"Temporal Analyzer initialized with {len(self.all_markers)} time markers")
    
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
        """Filter stories within a date range"""
        filtered = [
            s for s in stories 
            if start_date <= s['date'] < end_date
        ]
        return filtered
    
    def create_custom_periods(self, stories, period_definitions):
        """Create custom time periods"""
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
    
    def find_temporal_markers_in_text(self, text):
        """
        Find temporal markers in text
        
        Args:
            text: Text to search
            
        Returns:
            Dictionary of {category: [markers found]}
        """
        if not text:
            return {}
        
        text_lower = text.lower()
        found_markers = defaultdict(list)
        
        for category, markers in self.temporal_markers.items():
            for marker in markers:
                # Use word boundaries for better matching
                pattern = r'\b' + re.escape(marker) + r'\b'
                if re.search(pattern, text_lower):
                    found_markers[category].append(marker)
        
        return dict(found_markers)
    
    def analyze_temporal_markers(self, periods):
        """
        Analyze temporal marker frequencies across periods
        
        Args:
            periods: List of period dictionaries
            
        Returns:
            Dictionary with temporal marker analysis
        """
        print("\n" + "=" * 120)
        print("ANALYZING TEMPORAL MARKERS")
        print("=" * 120)
        
        # Count temporal markers per period
        period_marker_counts = []
        
        for period in periods:
            category_counts = defaultdict(int)
            marker_counts = Counter()
            stories_with_markers = []
            
            for story in period['stories']:
                # Check original title for temporal markers
                title = story.get('original_title', '')
                found = self.find_temporal_markers_in_text(title)
                
                if found:
                    stories_with_markers.append({
                        'title': title,
                        'date': story['date'],
                        'markers': found
                    })
                    
                    for category, markers in found.items():
                        category_counts[category] += len(markers)
                        marker_counts.update(markers)
            
            period_marker_counts.append({
                'period': period,
                'category_counts': dict(category_counts),
                'marker_counts': marker_counts,
                'stories_with_markers': stories_with_markers,
                'total_markers': sum(category_counts.values())
            })
            
            print(f"\n{period['label']}:")
            print(f"  Stories with temporal markers: {len(stories_with_markers)}")
            print(f"  Total temporal markers found: {sum(category_counts.values())}")
        
        return period_marker_counts
    
    def calculate_temporal_statistics(self, period_marker_counts):
        """
        Calculate z-scores and changes for temporal markers
        
        Args:
            period_marker_counts: List of period marker data
            
        Returns:
            Dictionary of statistics per marker
        """
        # Get all unique markers across all periods
        all_markers = set()
        for pmc in period_marker_counts:
            all_markers.update(pmc['marker_counts'].keys())
        
        marker_stats = {}
        
        for marker in all_markers:
            # Get counts across periods
            counts = [pmc['marker_counts'].get(marker, 0) for pmc in period_marker_counts]
            
            # Baseline (first period)
            baseline = counts[0]
            baseline_mean = baseline
            baseline_stdev = baseline * 0.3 if baseline > 0 else 1.0
            
            # Z-scores for subsequent periods
            z_scores = []
            for i in range(1, len(counts)):
                if baseline_stdev > 0:
                    z = (counts[i] - baseline_mean) / baseline_stdev
                else:
                    z = 10.0 if counts[i] > baseline_mean else 0.0
                z_scores.append(z)
            
            # Velocity
            velocities = []
            for i in range(1, len(counts)):
                vel = counts[i] - counts[i-1]
                velocities.append(vel)
            
            # Determine category
            category = None
            for cat, markers in self.temporal_markers.items():
                if marker in markers:
                    category = cat
                    break
            
            marker_stats[marker] = {
                'counts': counts,
                'baseline_mean': baseline_mean,
                'z_scores': z_scores,
                'max_z_score': max(z_scores) if z_scores else 0,
                'velocity': velocities[-1] if velocities else 0,
                'category': category
            }
        
        return marker_stats
    
    def display_temporal_analysis(self, period_marker_counts, marker_stats, periods, top_n=20):
        """Display temporal marker analysis"""
        print("\n" + "=" * 120)
        print("TEMPORAL MARKER FREQUENCY ANALYSIS")
        print("=" * 120)
        
        # Sort by max z-score
        sorted_markers = sorted(
            marker_stats.items(),
            key=lambda x: x[1]['max_z_score'],
            reverse=True
        )
        
        # Header
        header = f"{'Temporal Marker':<25}{'Category':<15}"
        for period in periods:
            header += f"{period['label'][:10]:>12}"
        header += f"{'Max Z':>10}{'Velocity':>10}{'Signal':>15}"
        
        print(header)
        print("-" * 120)
        
        # Display top markers
        for marker, stats in sorted_markers[:top_n]:
            row = f"{marker:<25}{stats['category']:<15}"
            
            for count in stats['counts']:
                row += f"{count:>12}"
            
            row += f"{stats['max_z_score']:>10.2f}"
            row += f"{stats['velocity']:>10}"
            
            z = stats['max_z_score']
            if z > 4.0:
                signal = "🔥 VERY STRONG"
            elif z > 3.0:
                signal = "⚡ STRONG"
            elif z > 2.5:
                signal = "✓ HIGH"
            elif z > 2.0:
                signal = "• Medium"
            else:
                signal = "- Weak"
            
            row += f"{signal:>15}"
            
            print(row)
        
        print("=" * 120)
    
    def find_temporal_context(self, periods, min_z_score=2.0):
        """
        Find stories with temporal markers and high z-score words together
        
        Args:
            periods: List of period dictionaries
            min_z_score: Minimum z-score for context analysis
            
        Returns:
            List of contextual findings
        """
        print("\n" + "=" * 120)
        print("TEMPORAL CONTEXT ANALYSIS")
        print("=" * 120)
        print("Stories with temporal markers AND significant word changes")
        print("=" * 120)
        
        # For monitoring period (period 1)
        monitoring = periods[1]
        
        context_findings = []
        
        for story in monitoring['stories']:
            title = story.get('original_title', '')
            words = story['words'].split('|') if story['words'] else []
            
            # Find temporal markers
            temporal_found = self.find_temporal_markers_in_text(title)
            
            if temporal_found:
                context_findings.append({
                    'title': title,
                    'date': story['date'],
                    'temporal_markers': temporal_found,
                    'words': words[:10]  # Top 10 words
                })
        
        # Display findings
        print(f"\nFound {len(context_findings)} stories with temporal context in monitoring period\n")
        
        for i, finding in enumerate(context_findings[:15], 1):
            print(f"{i}. [{finding['date'].strftime('%Y-%m-%d')}] {finding['title']}")
            
            markers_str = []
            for category, markers in finding['temporal_markers'].items():
                markers_str.append(f"{category}: {', '.join(markers)}")
            
            print(f"   Temporal: {' | '.join(markers_str)}")
            print(f"   Key words: {', '.join(finding['words'][:5])}")
            print()
        
        return context_findings
    
    def save_temporal_analysis_to_csv(self, marker_stats, periods, output_file):
        """Save temporal analysis to CSV"""
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['temporal_marker', 'category', 'baseline_count']
            
            # Add period columns
            for period in periods:
                fieldnames.append(period['label'])
            
            fieldnames.extend(['max_z_score', 'velocity', 'signal_strength'])
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for marker, stats in marker_stats.items():
                row = {
                    'temporal_marker': marker,
                    'category': stats['category'],
                    'baseline_count': stats['counts'][0],
                    'max_z_score': round(stats['max_z_score'], 2),
                    'velocity': stats['velocity']
                }
                
                # Add period counts
                for i, period in enumerate(periods):
                    row[period['label']] = stats['counts'][i]
                
                # Signal strength
                z = stats['max_z_score']
                if z > 4.0:
                    row['signal_strength'] = "VERY STRONG"
                elif z > 3.0:
                    row['signal_strength'] = "STRONG"
                elif z > 2.5:
                    row['signal_strength'] = "HIGH"
                elif z > 2.0:
                    row['signal_strength'] = "Medium"
                else:
                    row['signal_strength'] = "Weak"
                
                writer.writerow(row)
        
        print(f"\n✅ Temporal analysis saved to: {output_file}")


# Main execution
if __name__ == "__main__":
    print("=" * 120)
    print("TEMPORAL MARKER ANALYZER")
    print("=" * 120)
    
    input_file = 'data/processed/hackernews_training_processed.csv'
    
    analyzer = TemporalAnalyzer()
    
    # Load data
    stories = analyzer.load_processed_data(input_file)
    
    # Define periods
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
    
    # Analyze temporal markers
    period_marker_counts = analyzer.analyze_temporal_markers(periods)
    
    # Calculate statistics
    marker_stats = analyzer.calculate_temporal_statistics(period_marker_counts)
    
    # Display analysis
    analyzer.display_temporal_analysis(period_marker_counts, marker_stats, periods, top_n=25)
    
    # Find temporal context
    context_findings = analyzer.find_temporal_context(periods, min_z_score=2.0)
    
    # Save to CSV
    output_csv = 'data/analysis/temporal_markers_may_june_july_2024.csv'
    analyzer.save_temporal_analysis_to_csv(marker_stats, periods, output_csv)
    
    print("\n" + "=" * 120)
    print("Done!")
    print(f"Source file: {input_file}")
    print(f"Output CSV: {output_csv}")
    print(f"Date ranges: {', '.join([p['label'] for p in periods])}")
    print("\nNEXT: Review temporal markers to identify WHEN predicted events might occur!")
    print("=" * 120)