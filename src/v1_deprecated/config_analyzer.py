# config_analyzer.py - Master analyzer that runs from configuration files
# Allows reproducible experiments with saved configs

import yaml
import csv
from datetime import datetime
import os
import sys

# Import our analyzers
from analyze_date_range import DateRangeAnalyzer
from cluster_analyzer import ClusterAnalyzer
from topic_analyzer import TopicAnalyzer
from semantic_analyzer import SemanticAnalyzer
from temporal_analyzer import TemporalAnalyzer

class ConfigAnalyzer:
    """
    Master analyzer that runs all analysis tools based on configuration files
    """
    
    def __init__(self, config_path):
        """
        Initialize with configuration file
        
        Args:
            config_path: Path to YAML configuration file
        """
        print(f"Loading configuration from: {config_path}")
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        print("✓ Configuration loaded")
        self.validate_config()
    
    def validate_config(self):
        """Validate configuration has required fields"""
        required = ['date_ranges', 'parameters', 'files', 'analyzers']
        
        for field in required:
            if field not in self.config:
                raise ValueError(f"Missing required config section: {field}")
        
        # Validate date ranges
        dr = self.config['date_ranges']
        required_dates = ['baseline_start', 'baseline_end', 'monitoring_start', 
                         'monitoring_end', 'event_start', 'event_end']
        
        for date_field in required_dates:
            if date_field not in dr:
                raise ValueError(f"Missing required date: {date_field}")
            
            # Try parsing date
            try:
                datetime.strptime(dr[date_field], '%Y-%m-%d')
            except ValueError:
                raise ValueError(f"Invalid date format for {date_field}: {dr[date_field]}")
        
        print("✓ Configuration validated")
    
    def get_periods(self):
        """
        Get period definitions from config
        
        Returns:
            List of period tuples (label, start_date, end_date)
        """
        dr = self.config['date_ranges']
        
        periods = [
            (
                "Baseline",
                datetime.strptime(dr['baseline_start'], '%Y-%m-%d'),
                datetime.strptime(dr['baseline_end'], '%Y-%m-%d')
            ),
            (
                "Monitoring",
                datetime.strptime(dr['monitoring_start'], '%Y-%m-%d'),
                datetime.strptime(dr['monitoring_end'], '%Y-%m-%d')
            ),
            (
                "Event",
                datetime.strptime(dr['event_start'], '%Y-%m-%d'),
                datetime.strptime(dr['event_end'], '%Y-%m-%d')
            )
        ]
        
        return periods
    
    def run_word_frequency_analysis(self, periods):
        """Run word frequency analyzer"""
        print("\n" + "=" * 120)
        print("WORD FREQUENCY ANALYSIS")
        print("=" * 120)
        
        analyzer = DateRangeAnalyzer()
        stories = analyzer.load_processed_data(self.config['files']['input'])
        period_objs = analyzer.create_custom_periods(stories, periods)
        
        params = self.config['parameters']
        word_stats = analyzer.create_comparison_table(
            period_objs,
            min_z_score=params.get('min_z_score', 2.0),
            top_n=params.get('top_n_words', 40)
        )
        
        # Save results
        output_file = os.path.join(
            self.config['files']['output_dir'],
            f"{self.config['files']['output_prefix']}_word_frequency.csv"
        )
        
        # Create output directory
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Save to CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['word', 'baseline_mean', 'baseline_stdev']
            for p in period_objs:
                fieldnames.append(p['label'])
            fieldnames.extend(['max_z_score', 'velocity', 'acceleration', 'signal_strength'])
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for item in word_stats:
                row = {
                    'word': item['word'],
                    'baseline_mean': round(item['baseline_mean'], 2),
                    'baseline_stdev': round(item.get('baseline_stdev', 0), 2),
                    'max_z_score': round(item['max_z_score'], 2),
                    'velocity': item['velocity'],
                    'acceleration': item['acceleration']
                }
                
                for i, p in enumerate(period_objs):
                    row[p['label']] = item['counts'][i]
                
                z = item['max_z_score']
                if z > 4.0:
                    row['signal_strength'] = "VERY STRONG"
                elif z > 3.0:
                    row['signal_strength'] = "STRONG"
                elif z > 2.5:
                    row['signal_strength'] = "HIGH"
                else:
                    row['signal_strength'] = "Medium"
                
                writer.writerow(row)
        
        print(f"✓ Saved to: {output_file}")
        return word_stats
    
    def run_cluster_analysis(self, periods):
        """Run cluster analyzer"""
        print("\n" + "=" * 120)
        print("CO-OCCURRENCE CLUSTER ANALYSIS")
        print("=" * 120)
        
        analyzer = ClusterAnalyzer()
        stories = analyzer.load_processed_data(self.config['files']['input'])
        period_objs = analyzer.create_custom_periods(stories, periods)
        
        params = self.config['parameters']
        clusters = analyzer.find_clusters(
            period_objs,
            min_cluster_size=params.get('min_cluster_size', 2),
            min_z_score=params.get('min_z_score', 2.0),
            min_cooccurrence=params.get('min_cooccurrence', 3)
        )
        
        if clusters:
            analyzer.display_clusters(clusters, period_objs, top_n=params.get('top_n_clusters', 15))
            
            # Save results
            output_file = os.path.join(
                self.config['files']['output_dir'],
                f"{self.config['files']['output_prefix']}_clusters.csv"
            )
            analyzer.save_clusters_to_csv(clusters, period_objs, output_file)
            print(f"✓ Saved to: {output_file}")
        else:
            print("No clusters found with current parameters")
        
        return clusters
    
    def run_topic_analysis(self, periods):
        """Run topic analyzer"""
        print("\n" + "=" * 120)
        print("TOPIC MODELING ANALYSIS")
        print("=" * 120)
        
        params = self.config['parameters']
        analyzer = TopicAnalyzer(
            n_topics=params.get('n_topics', 12),
            top_words=params.get('top_words_per_topic', 10)
        )
        
        stories = analyzer.load_processed_data(self.config['files']['input'])
        period_objs = analyzer.create_custom_periods(stories, periods)
        
        topic_stats, topics_data = analyzer.analyze_topic_changes(period_objs, stories)
        analyzer.display_topic_analysis(topic_stats, period_objs, top_n=params.get('top_n_topics', 12))
        
        # Save results
        output_file = os.path.join(
            self.config['files']['output_dir'],
            f"{self.config['files']['output_prefix']}_topics.csv"
        )
        analyzer.save_topic_analysis_to_csv(topic_stats, topics_data, period_objs, output_file)
        print(f"✓ Saved to: {output_file}")
        
        return topic_stats
    
    def run_semantic_analysis(self, periods):
        """Run semantic analyzer"""
        print("\n" + "=" * 120)
        print("SEMANTIC SIMILARITY ANALYSIS")
        print("=" * 120)
        
        params = self.config['parameters']
        analyzer = SemanticAnalyzer(
            similarity_threshold=params.get('similarity_threshold', 0.6)
        )
        
        stories = analyzer.load_processed_data(self.config['files']['input'])
        period_objs = analyzer.create_custom_periods(stories, periods)
        
        clusters = analyzer.find_semantic_clusters(
            period_objs,
            min_z_score=params.get('min_z_score', 2.0),
            min_word_freq=params.get('min_word_freq', 5),
            max_cluster_size=params.get('max_cluster_size', 10)
        )
        
        if clusters:
            analyzer.display_semantic_clusters(clusters, period_objs, top_n=params.get('top_n_clusters', 15))
            
            # Save results
            output_file = os.path.join(
                self.config['files']['output_dir'],
                f"{self.config['files']['output_prefix']}_semantic.csv"
            )
            analyzer.save_clusters_to_csv(clusters, period_objs, output_file)
            print(f"✓ Saved to: {output_file}")
        else:
            print("No semantic clusters found with current parameters")
        
        return clusters
    
    def run_temporal_analysis(self, periods):
        """Run temporal analyzer"""
        print("\n" + "=" * 120)
        print("TEMPORAL MARKER ANALYSIS")
        print("=" * 120)
        
        analyzer = TemporalAnalyzer()
        stories = analyzer.load_processed_data(self.config['files']['input'])
        period_objs = analyzer.create_custom_periods(stories, periods)
        
        period_marker_counts = analyzer.analyze_temporal_markers(period_objs)
        marker_stats = analyzer.calculate_temporal_statistics(period_marker_counts)
        
        params = self.config['parameters']
        analyzer.display_temporal_analysis(
            period_marker_counts, 
            marker_stats, 
            period_objs, 
            top_n=params.get('top_n_temporal', 25)
        )
        
        # Context analysis
        analyzer.find_temporal_context(period_objs, min_z_score=params.get('min_z_score', 2.0))
        
        # Save results
        output_file = os.path.join(
            self.config['files']['output_dir'],
            f"{self.config['files']['output_prefix']}_temporal.csv"
        )
        analyzer.save_temporal_analysis_to_csv(marker_stats, period_objs, output_file)
        print(f"✓ Saved to: {output_file}")
        
        return marker_stats
    
    def run_all_analyses(self):
        """Run all enabled analyzers from config"""
        print("\n" + "=" * 120)
        print("RUNNING CONFIGURED ANALYSES")
        print("=" * 120)
        
        # Display config summary
        print("\nConfiguration Summary:")
        print(f"  Input: {self.config['files']['input']}")
        print(f"  Output: {self.config['files']['output_dir']}/{self.config['files']['output_prefix']}_*.csv")
        
        periods = self.get_periods()
        print("\nDate Ranges:")
        for label, start, end in periods:
            days = (end - start).days
            print(f"  {label:12s}: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')} ({days} days)")
        
        print("\nParameters:")
        for key, value in self.config['parameters'].items():
            print(f"  {key}: {value}")
        
        print("\nEnabled Analyzers:")
        enabled = self.config['analyzers']
        for analyzer, is_enabled in enabled.items():
            status = "✓" if is_enabled else "✗"
            print(f"  {status} {analyzer}")
        
        # Run enabled analyzers
        results = {}
        
        if enabled.get('word_frequency', True):
            results['word_frequency'] = self.run_word_frequency_analysis(periods)
        
        if enabled.get('cluster', True):
            results['cluster'] = self.run_cluster_analysis(periods)
        
        if enabled.get('topic', True):
            results['topic'] = self.run_topic_analysis(periods)
        
        if enabled.get('semantic', True):
            results['semantic'] = self.run_semantic_analysis(periods)
        
        if enabled.get('temporal', True):
            results['temporal'] = self.run_temporal_analysis(periods)
        
        return results


# Main execution
if __name__ == "__main__":
    print("=" * 120)
    print("CONFIGURATION-DRIVEN LINGUISTIC PREDICTOR")
    print("=" * 120)
    
    # Check for config file argument
    if len(sys.argv) < 2:
        print("\nUsage: python3 config_analyzer.py <config_file.yaml>")
        print("\nExample: python3 config_analyzer.py configs/may_june_july_2024.yaml")
        print("\nNo config file provided, using default: configs/default.yaml")
        config_path = 'configs/default.yaml'
    else:
        config_path = sys.argv[1]
    
    # Check if config exists
    if not os.path.exists(config_path):
        print(f"\nERROR: Config file not found: {config_path}")
        print("\nAvailable configs:")
        if os.path.exists('configs'):
            for f in os.listdir('configs'):
                if f.endswith('.yaml'):
                    print(f"  - configs/{f}")
        sys.exit(1)
    
    # Run analysis
    analyzer = ConfigAnalyzer(config_path)
    results = analyzer.run_all_analyses()
    
    print("\n" + "=" * 120)
    print("ANALYSIS COMPLETE")
    print("=" * 120)
    print(f"\nAll results saved to: {analyzer.config['files']['output_dir']}/")
    print(f"Config used: {config_path}")
    print("\nNEXT: Review output CSV files in Google Sheets!")
    print("=" * 120)
