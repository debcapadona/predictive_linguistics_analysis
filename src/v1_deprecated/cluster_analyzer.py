# cluster_analyzer.py - Find word clusters that co-occur and spike together
# Phase 1: Co-occurrence Analysis

import csv
from collections import Counter, defaultdict
from datetime import datetime
import statistics
import os
from itertools import combinations

class ClusterAnalyzer:
    """
    Analyzes word co-occurrence patterns and identifies clusters that spike together
    """
    
    def __init__(self):
        """Initialize the analyzer"""
        print("Co-occurrence Cluster Analyzer initialized")
    
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
    
    def build_cooccurrence_matrix(self, period, min_word_freq=5):
        """
        Build co-occurrence matrix for a period
        
        Args:
            period: Period dictionary with stories
            min_word_freq: Minimum frequency for a word to be considered
            
        Returns:
            Dictionary of word pairs -> co-occurrence count
        """
        # First, get word frequencies to filter rare words
        word_freq = Counter()
        for story in period['stories']:
            words = story['words'].split('|') if story['words'] else []
            word_freq.update(words)
        
        # Filter to words that appear at least min_word_freq times
        valid_words = {word for word, count in word_freq.items() if count >= min_word_freq}
        
        # Build co-occurrence matrix
        cooccurrence = defaultdict(int)
        
        for story in period['stories']:
            words = story['words'].split('|') if story['words'] else []
            # Only keep valid words
            words = [w for w in words if w in valid_words]
            # Remove duplicates within same story
            words = list(set(words))
            
            # Count all pairs
            for word1, word2 in combinations(sorted(words), 2):
                cooccurrence[(word1, word2)] += 1
        
        return dict(cooccurrence), word_freq
    
    def calculate_word_statistics(self, periods, word, baseline_periods=1):
        """Calculate z-score and other stats for a word across periods"""
        # Count word frequency in each period
        counts = []
        for period in periods:
            count = 0
            for story in period['stories']:
                words = story['words'].split('|') if story['words'] else []
                count += words.count(word)
            counts.append(count)
        
        # Baseline stats
        baseline = counts[:baseline_periods]
        
        if not baseline or all(c == 0 for c in baseline):
            return {
                'counts': counts,
                'baseline_mean': 0,
                'max_z_score': 0
            }
        
        baseline_mean = statistics.mean(baseline)
        
        if len(baseline) > 1:
            baseline_stdev = statistics.stdev(baseline)
        else:
            baseline_stdev = baseline_mean * 0.3 if baseline_mean > 0 else 1.0
        
        # Z-scores for periods after baseline
        z_scores = []
        for i in range(baseline_periods, len(counts)):
            if baseline_stdev > 0:
                z = (counts[i] - baseline_mean) / baseline_stdev
            else:
                z = 10.0 if counts[i] > baseline_mean else 0.0
            z_scores.append(z)
        
        return {
            'counts': counts,
            'baseline_mean': baseline_mean,
            'baseline_stdev': baseline_stdev,
            'z_scores': z_scores,
            'max_z_score': max(z_scores) if z_scores else 0
        }
    
    def find_clusters(self, periods, min_cluster_size=3, min_z_score=2.5, min_cooccurrence=5):
        """
        Find word clusters that co-occur and spike together
        
        Args:
            periods: List of period dictionaries
            min_cluster_size: Minimum number of words in a cluster
            min_z_score: Minimum z-score for words to be considered
            min_cooccurrence: Minimum times words must co-occur
            
        Returns:
            List of cluster dictionaries
        """
        print("\n" + "=" * 120)
        print("FINDING CO-OCCURRENCE CLUSTERS")
        print("=" * 120)
        print(f"Settings: min_cluster_size={min_cluster_size}, min_z_score={min_z_score}, min_cooccurrence={min_cooccurrence}")
        
        # Build co-occurrence matrices for each period
        print("\nBuilding co-occurrence matrices...")
        period_cooccurrences = []
        for period in periods:
            cooc, word_freq = self.build_cooccurrence_matrix(period, min_word_freq=5)
            period_cooccurrences.append({
                'period': period,
                'cooccurrence': cooc,
                'word_freq': word_freq
            })
            print(f"  {period['label']}: {len(cooc)} word pairs with co-occurrence >= {min_cooccurrence}")
        
        # Find words with high z-scores in monitoring/event periods
        print("\nCalculating z-scores for words...")
        high_z_words = set()
        word_stats = {}
        
        # Get all unique words across all periods
        all_words = set()
        for pc in period_cooccurrences:
            all_words.update(pc['word_freq'].keys())
        
        for word in all_words:
            if not word:
                continue
            stats = self.calculate_word_statistics(periods, word, baseline_periods=1)
            word_stats[word] = stats
            
            if stats['max_z_score'] >= min_z_score:
                high_z_words.add(word)
        
        print(f"  Found {len(high_z_words)} words with z-score >= {min_z_score}")
        
        # Find clusters: groups of high-z words that co-occur frequently
        print("\nFinding clusters...")
        clusters = []
        
        # Use the monitoring period (period 1, after baseline) for co-occurrence
        monitoring_cooc = period_cooccurrences[1]['cooccurrence']
        
        # Build adjacency graph of high-z words that co-occur
        word_connections = defaultdict(set)
        for (word1, word2), count in monitoring_cooc.items():
            if count >= min_cooccurrence:
                if word1 in high_z_words and word2 in high_z_words:
                    word_connections[word1].add(word2)
                    word_connections[word2].add(word1)
        
        # Find connected components (clusters)
        visited = set()
        cluster_id = 0
        
        for word in word_connections:
            if word in visited:
                continue
            
            # BFS to find connected component
            cluster_words = set()
            queue = [word]
            
            while queue:
                current = queue.pop(0)
                if current in visited:
                    continue
                
                visited.add(current)
                cluster_words.add(current)
                
                for neighbor in word_connections[current]:
                    if neighbor not in visited:
                        queue.append(neighbor)
            
            # Only keep clusters of minimum size
            if len(cluster_words) >= min_cluster_size:
                cluster_id += 1
                
                # Calculate cluster statistics
                cluster_z_scores = [word_stats[w]['max_z_score'] for w in cluster_words]
                avg_z = statistics.mean(cluster_z_scores)
                max_z = max(cluster_z_scores)
                
                # Get word frequencies across periods
                cluster_data = {
                    'cluster_id': cluster_id,
                    'words': sorted(cluster_words),
                    'size': len(cluster_words),
                    'avg_z_score': avg_z,
                    'max_z_score': max_z,
                    'word_details': {}
                }
                
                # Add detailed stats for each word
                for w in cluster_words:
                    cluster_data['word_details'][w] = word_stats[w]
                
                clusters.append(cluster_data)
        
        # Sort clusters by average z-score
        clusters.sort(key=lambda x: x['avg_z_score'], reverse=True)
        
        print(f"  Found {len(clusters)} clusters")
        
        return clusters
    
    def display_clusters(self, clusters, periods, top_n=10):
        """Display top clusters in readable format"""
        print("\n" + "=" * 120)
        print(f"TOP {top_n} WORD CLUSTERS (Ranked by Average Z-Score)")
        print("=" * 120)
        
        for cluster in clusters[:top_n]:
            print(f"\n{'=' * 120}")
            print(f"CLUSTER #{cluster['cluster_id']} | {cluster['size']} words | Avg Z-Score: {cluster['avg_z_score']:.2f} | Max Z-Score: {cluster['max_z_score']:.2f}")
            print(f"{'=' * 120}")
            
            # Display words in table
            print(f"\n{'Word':<20}", end='')
            for period in periods:
                print(f"{period['label'][:10]:>12}", end='')
            print(f"{'Z-Score':>12}{'Signal':>15}")
            print("-" * 120)
            
            for word in cluster['words']:
                details = cluster['word_details'][word]
                print(f"{word:<20}", end='')
                
                for count in details['counts']:
                    print(f"{count:>12}", end='')
                
                z = details['max_z_score']
                print(f"{z:>12.2f}", end='')
                
                if z > 4.0:
                    signal = "🔥 VERY STRONG"
                elif z > 3.0:
                    signal = "⚡ STRONG"
                elif z > 2.5:
                    signal = "✓ HIGH"
                else:
                    signal = "• Medium"
                
                print(f"{signal:>15}")
            
            print()
    
    def save_clusters_to_csv(self, clusters, periods, output_file):
        """Save clusters to CSV for analysis"""
        # Create directory if needed
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['cluster_id', 'cluster_size', 'avg_z_score', 'max_z_score', 
                         'word', 'baseline_mean']
            
            # Add period columns
            for period in periods:
                fieldnames.append(period['label'])
            
            fieldnames.extend(['word_z_score', 'signal_strength'])
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for cluster in clusters:
                for word in cluster['words']:
                    details = cluster['word_details'][word]
                    
                    row = {
                        'cluster_id': cluster['cluster_id'],
                        'cluster_size': cluster['size'],
                        'avg_z_score': round(cluster['avg_z_score'], 2),
                        'max_z_score': round(cluster['max_z_score'], 2),
                        'word': word,
                        'baseline_mean': round(details['baseline_mean'], 2),
                        'word_z_score': round(details['max_z_score'], 2)
                    }
                    
                    # Add period counts
                    for i, period in enumerate(periods):
                        row[period['label']] = details['counts'][i]
                    
                    # Signal strength
                    z = details['max_z_score']
                    if z > 4.0:
                        row['signal_strength'] = "VERY STRONG"
                    elif z > 3.0:
                        row['signal_strength'] = "STRONG"
                    elif z > 2.5:
                        row['signal_strength'] = "HIGH"
                    else:
                        row['signal_strength'] = "Medium"
                    
                    writer.writerow(row)
        
        print(f"\n✅ Clusters saved to: {output_file}")


# Main execution
if __name__ == "__main__":
    print("=" * 120)
    print("CO-OCCURRENCE CLUSTER ANALYZER")
    print("=" * 120)
    
    input_file = 'data/processed/hackernews_training_processed.csv'
    
    analyzer = ClusterAnalyzer()
    
    # Load data
    stories = analyzer.load_processed_data(input_file)
    
    # Define periods (same as date range analyzer)
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
    
    # Find clusters
    clusters = analyzer.find_clusters(
        periods,
        min_cluster_size=2,      # At least 2 words per cluster (lowered from 3)
        min_z_score=2.0,         # Only words with z >= 2.0 (lowered from 2.5)
        min_cooccurrence=3       # Words must co-occur in at least 3 stories (lowered from 5)
    )
    
    # Display results
    analyzer.display_clusters(clusters, periods, top_n=15)
    
    # Save to CSV
    output_csv = 'data/analysis/clusters_may_june_july_2024.csv'
    analyzer.save_clusters_to_csv(clusters, periods, output_csv)
    
    print("\n" + "=" * 120)
    print("Done!")
    print(f"Source file: {input_file}")
    print(f"Output CSV: {output_csv}")
    print(f"Clusters found: {len(clusters)}")
    print(f"Date ranges: {', '.join([p['label'] for p in periods])}")
    print("\nNEXT: Review clusters in Google Sheets and compare to July 2024 events!")
    print("=" * 120)