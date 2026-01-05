# semantic_analyzer.py - Semantic similarity analysis using word embeddings
# Phase 3: Semantic Similarity (Word Embeddings)

import csv
from collections import Counter, defaultdict
from datetime import datetime
import statistics
import os
import spacy
from itertools import combinations
import numpy as np

class SemanticAnalyzer:
    """
    Analyzes semantic similarity between words using word embeddings
    Finds semantically related clusters and tracks their frequency over time
    """
    
    def __init__(self, similarity_threshold=0.6):
        """
        Initialize the analyzer
        
        Args:
            similarity_threshold: Minimum cosine similarity to consider words related (0-1)
        """
        self.similarity_threshold = similarity_threshold
        print("Loading spaCy model with word vectors...")
        self.nlp = spacy.load('en_core_web_md')
        print(f"✓ Semantic Analyzer initialized (similarity threshold: {similarity_threshold})")
    
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
    
    def get_word_vector(self, word):
        """
        Get word vector from spaCy
        
        Args:
            word: String word
            
        Returns:
            Word vector or None if not in vocabulary
        """
        doc = self.nlp(word)
        if doc and doc[0].has_vector:
            return doc[0].vector
        return None
    
    def calculate_similarity(self, word1, word2):
        """
        Calculate cosine similarity between two words
        
        Args:
            word1, word2: Words to compare
            
        Returns:
            Similarity score (0-1) or 0 if vectors not available
        """
        vec1 = self.get_word_vector(word1)
        vec2 = self.get_word_vector(word2)
        
        if vec1 is None or vec2 is None:
            return 0.0
        
        # Cosine similarity
        similarity = np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
        return similarity
    
    def count_words_in_period(self, period):
        """Count word frequencies for a period"""
        all_words = []
        
        for story in period['stories']:
            words = story['words'].split('|') if story['words'] else []
            all_words.extend(words)
        
        return Counter(all_words)
    
    def calculate_word_statistics(self, periods, word, baseline_periods=1):
        """Calculate z-score and other stats for a word across periods"""
        # Count word frequency in each period
        counts = []
        for period in periods:
            word_freq = self.count_words_in_period(period)
            counts.append(word_freq.get(word, 0))
        
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
        
        # Z-scores
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
    
    def find_semantic_clusters(self, periods, min_z_score=2.0, min_word_freq=5, max_cluster_size=10):
        """
        Find semantically similar word clusters that spike together
        
        Args:
            periods: List of period dictionaries
            min_z_score: Minimum z-score for words to be considered
            min_word_freq: Minimum frequency in monitoring period
            max_cluster_size: Maximum words per cluster
            
        Returns:
            List of semantic cluster dictionaries
        """
        print("\n" + "=" * 120)
        print("FINDING SEMANTIC CLUSTERS")
        print("=" * 120)
        print(f"Settings: min_z_score={min_z_score}, similarity_threshold={self.similarity_threshold}")
        
        # Get high z-score words from monitoring/event periods
        print("\nFinding words with high z-scores...")
        high_z_words = {}
        word_stats_cache = {}
        
        # Get word frequencies in monitoring period to filter rare words
        monitoring_freq = self.count_words_in_period(periods[1])
        
        for word in monitoring_freq:
            if not word or monitoring_freq[word] < min_word_freq:
                continue
            
            stats = self.calculate_word_statistics(periods, word, baseline_periods=1)
            word_stats_cache[word] = stats
            
            if stats['max_z_score'] >= min_z_score:
                high_z_words[word] = stats['max_z_score']
        
        print(f"  Found {len(high_z_words)} words with z-score >= {min_z_score}")
        
        if not high_z_words:
            print("  No high z-score words found!")
            return []
        
        # Calculate semantic similarity between high-z words
        print("\nCalculating semantic similarities...")
        word_list = list(high_z_words.keys())
        similarity_matrix = {}
        
        for i, word1 in enumerate(word_list):
            if i % 50 == 0:
                print(f"  Processed {i}/{len(word_list)} words...")
            
            for word2 in word_list[i+1:]:
                similarity = self.calculate_similarity(word1, word2)
                if similarity >= self.similarity_threshold:
                    similarity_matrix[(word1, word2)] = similarity
        
        print(f"  Found {len(similarity_matrix)} similar word pairs")
        
        # Build semantic clusters using graph connected components
        print("\nBuilding semantic clusters...")
        word_connections = defaultdict(set)
        
        for (word1, word2), similarity in similarity_matrix.items():
            word_connections[word1].add(word2)
            word_connections[word2].add(word1)
        
        # Find connected components (clusters)
        visited = set()
        clusters = []
        cluster_id = 0
        
        for word in word_connections:
            if word in visited:
                continue
            
            # BFS to find connected component
            cluster_words = set()
            queue = [word]
            
            while queue and len(cluster_words) < max_cluster_size:
                current = queue.pop(0)
                if current in visited:
                    continue
                
                visited.add(current)
                cluster_words.add(current)
                
                for neighbor in word_connections[current]:
                    if neighbor not in visited and len(cluster_words) < max_cluster_size:
                        queue.append(neighbor)
            
            # Only keep clusters with at least 2 words
            if len(cluster_words) >= 2:
                cluster_id += 1
                
                # Calculate cluster statistics
                cluster_z_scores = [word_stats_cache[w]['max_z_score'] for w in cluster_words]
                avg_z = statistics.mean(cluster_z_scores)
                max_z = max(cluster_z_scores)
                
                # Calculate total cluster frequency across periods
                cluster_counts = [0] * len(periods)
                for w in cluster_words:
                    word_counts = word_stats_cache[w]['counts']
                    for i in range(len(periods)):
                        cluster_counts[i] += word_counts[i]
                
                clusters.append({
                    'cluster_id': cluster_id,
                    'words': sorted(cluster_words, key=lambda w: word_stats_cache[w]['max_z_score'], reverse=True),
                    'size': len(cluster_words),
                    'avg_z_score': avg_z,
                    'max_z_score': max_z,
                    'cluster_counts': cluster_counts,
                    'word_details': {w: word_stats_cache[w] for w in cluster_words}
                })
        
        # Sort by average z-score
        clusters.sort(key=lambda x: x['avg_z_score'], reverse=True)
        
        print(f"  Found {len(clusters)} semantic clusters")
        
        return clusters
    
    def display_semantic_clusters(self, clusters, periods, top_n=10):
        """Display semantic clusters"""
        print("\n" + "=" * 120)
        print(f"TOP {top_n} SEMANTIC CLUSTERS (Ranked by Average Z-Score)")
        print("=" * 120)
        print("These are groups of semantically similar words that spiked together")
        print("=" * 120)
        
        for cluster in clusters[:top_n]:
            print(f"\n{'=' * 120}")
            print(f"CLUSTER #{cluster['cluster_id']} | {cluster['size']} semantically related words")
            print(f"Avg Z-Score: {cluster['avg_z_score']:.2f} | Max Z-Score: {cluster['max_z_score']:.2f}")
            print(f"{'=' * 120}")
            
            # Display cluster theme (top 3 words)
            theme_words = ', '.join(cluster['words'][:3])
            print(f"Theme: {theme_words}")
            
            # Display word table
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
            
            # Show cluster total frequency
            print("-" * 120)
            print(f"{'CLUSTER TOTAL':<20}", end='')
            for count in cluster['cluster_counts']:
                print(f"{count:>12}", end='')
            print()
    
    def save_clusters_to_csv(self, clusters, periods, output_file):
        """Save semantic clusters to CSV"""
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['cluster_id', 'cluster_size', 'cluster_theme', 'avg_z_score', 
                         'max_z_score', 'word', 'baseline_mean']
            
            # Add period columns
            for period in periods:
                fieldnames.append(period['label'])
            
            fieldnames.extend(['word_z_score', 'signal_strength'])
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for cluster in clusters:
                theme = ', '.join(cluster['words'][:3])
                
                for word in cluster['words']:
                    details = cluster['word_details'][word]
                    
                    row = {
                        'cluster_id': cluster['cluster_id'],
                        'cluster_size': cluster['size'],
                        'cluster_theme': theme,
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
        
        print(f"\n✅ Semantic clusters saved to: {output_file}")


# Main execution
if __name__ == "__main__":
    print("=" * 120)
    print("SEMANTIC SIMILARITY ANALYZER (Word Embeddings)")
    print("=" * 120)
    
    input_file = 'data/processed/hackernews_training_processed.csv'
    
    analyzer = SemanticAnalyzer(similarity_threshold=0.6)
    
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
    
    # Find semantic clusters
    clusters = analyzer.find_semantic_clusters(
        periods,
        min_z_score=2.0,         # Words with z >= 2.0
        min_word_freq=5,         # Must appear at least 5 times in monitoring period
        max_cluster_size=10      # Max 10 words per cluster
    )
    
    # Display results
    if clusters:
        analyzer.display_semantic_clusters(clusters, periods, top_n=15)
        
        # Save to CSV
        output_csv = 'data/analysis/semantic_clusters_may_june_july_2024.csv'
        analyzer.save_clusters_to_csv(clusters, periods, output_csv)
    else:
        print("\nNo semantic clusters found with current parameters.")
        print("Try lowering min_z_score or similarity_threshold.")
    
    print("\n" + "=" * 120)
    print("Done!")
    print(f"Source file: {input_file}")
    if clusters:
        print(f"Output CSV: {output_csv}")
        print(f"Clusters found: {len(clusters)}")
    print(f"Date ranges: {', '.join([p['label'] for p in periods])}")
    print("\nNEXT: Review semantic clusters and compare to July 2024 events!")
    print("=" * 120)