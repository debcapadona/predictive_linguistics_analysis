# date_range_experiment.py - Test different date ranges to find optimal signal windows
# Runs all analyzers with various time windows and compares results

import csv
from datetime import datetime, timedelta
import os
import sys

# Import our analyzers
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analyze_date_range import DateRangeAnalyzer
from cluster_analyzer import ClusterAnalyzer
from topic_analyzer import TopicAnalyzer
from semantic_analyzer import SemanticAnalyzer
from temporal_analyzer import TemporalAnalyzer

class DateRangeExperiment:
    """
    Run experiments with different date ranges to find optimal signal windows
    """
    
    def __init__(self, input_file):
        self.input_file = input_file
        
    def define_experiments(self, event_date):
        """
        Define different date range experiments
        
        Args:
            event_date: The event date (July 2024 event)
            
        Returns:
            List of experiment configurations
        """
        experiments = []
        
        # Experiment 1: Monthly (current approach)
        # Baseline: 1 month, Monitoring: 1 month, Event: 1 month
        experiments.append({
            'name': 'Monthly (1-1-1)',
            'baseline_days': 31,
            'monitoring_days': 30,
            'event_days': 31,
            'description': 'May-June-July 2024'
        })
        
        # Experiment 2: Bi-weekly
        # Baseline: 2 weeks, Monitoring: 2 weeks, Event: 2 weeks
        experiments.append({
            'name': 'Bi-weekly (14-14-14)',
            'baseline_days': 14,
            'monitoring_days': 14,
            'event_days': 14,
            'description': 'Mid-June through mid-July'
        })
        
        # Experiment 3: Weekly
        # Baseline: 1 week, Monitoring: 1 week, Event: 1 week
        experiments.append({
            'name': 'Weekly (7-7-7)',
            'baseline_days': 7,
            'monitoring_days': 7,
            'event_days': 7,
            'description': 'Last 3 weeks around event'
        })
        
        # Experiment 4: Extended baseline
        # Baseline: 2 months, Monitoring: 1 month, Event: 1 month
        experiments.append({
            'name': 'Extended Baseline (60-30-31)',
            'baseline_days': 60,
            'monitoring_days': 30,
            'event_days': 31,
            'description': 'April-May baseline, June monitoring, July event'
        })
        
        # Experiment 5: Short intensive
        # Baseline: 3 days, Monitoring: 3 days, Event: 3 days
        experiments.append({
            'name': 'Short Intensive (3-3-3)',
            'baseline_days': 3,
            'monitoring_days': 3,
            'event_days': 3,
            'description': 'Days immediately around event'
        })
        
        # Experiment 6: Asymmetric - long baseline, short monitoring
        # Baseline: 45 days, Monitoring: 15 days, Event: 15 days
        experiments.append({
            'name': 'Asymmetric (45-15-15)',
            'baseline_days': 45,
            'monitoring_days': 15,
            'event_days': 15,
            'description': 'Long baseline for stable patterns'
        })
        
        return experiments
    
    def calculate_periods_for_experiment(self, exp, event_date):
        """
        Calculate actual date ranges for an experiment
        
        Args:
            exp: Experiment configuration
            event_date: Event date
            
        Returns:
            List of period tuples (label, start_date, end_date)
        """
        # Event period: event_date + event_days
        event_end = event_date + timedelta(days=exp['event_days'])
        
        # Monitoring period: ends at event_date
        monitoring_start = event_date - timedelta(days=exp['monitoring_days'])
        
        # Baseline period: ends at monitoring_start
        baseline_start = monitoring_start - timedelta(days=exp['baseline_days'])
        
        periods = [
            ("Baseline", baseline_start, monitoring_start),
            ("Monitoring", monitoring_start, event_date),
            ("Event", event_date, event_end)
        ]
        
        return periods
    
    def run_word_frequency_experiment(self, periods):
        """Run word frequency analyzer with given periods"""
        try:
            analyzer = DateRangeAnalyzer()
            stories = analyzer.load_processed_data(self.input_file)
            period_objs = analyzer.create_custom_periods(stories, periods)
            
            # Check if we have data in all periods
            if any(p['story_count'] == 0 for p in period_objs):
                return {'error': 'No data in one or more periods'}
            
            word_stats = analyzer.create_comparison_table(
                period_objs,
                min_z_score=2.0,
                top_n=10
            )
            
            # Count signals
            very_strong = len([w for w in word_stats if w['max_z_score'] > 4.0])
            strong = len([w for w in word_stats if 3.0 < w['max_z_score'] <= 4.0])
            high = len([w for w in word_stats if 2.5 < w['max_z_score'] <= 3.0])
            medium = len([w for w in word_stats if 2.0 < w['max_z_score'] <= 2.5])
            
            return {
                'total_signals': len(word_stats),
                'very_strong': very_strong,
                'strong': strong,
                'high': high,
                'medium': medium,
                'avg_z_score': sum(w['max_z_score'] for w in word_stats[:10]) / min(10, len(word_stats)) if word_stats else 0,
                'top_word': word_stats[0]['word'] if word_stats else 'None',
                'top_z': word_stats[0]['max_z_score'] if word_stats else 0
            }
        except Exception as e:
            return {'error': str(e)}
    
    def run_semantic_experiment(self, periods):
        """Run semantic analyzer with given periods"""
        try:
            analyzer = SemanticAnalyzer(similarity_threshold=0.6)
            stories = analyzer.load_processed_data(self.input_file)
            period_objs = analyzer.create_custom_periods(stories, periods)
            
            # Check if we have data
            if any(p['story_count'] == 0 for p in period_objs):
                return {'error': 'No data in one or more periods'}
            
            clusters = analyzer.find_semantic_clusters(
                period_objs,
                min_z_score=2.0,
                min_word_freq=3,  # Lower threshold for shorter periods
                max_cluster_size=10
            )
            
            if not clusters:
                return {'total_clusters': 0, 'avg_cluster_size': 0, 'avg_z_score': 0}
            
            return {
                'total_clusters': len(clusters),
                'avg_cluster_size': sum(c['size'] for c in clusters) / len(clusters),
                'avg_z_score': sum(c['avg_z_score'] for c in clusters) / len(clusters),
                'top_cluster_words': ', '.join(clusters[0]['words'][:3]) if clusters else 'None',
                'top_cluster_z': clusters[0]['avg_z_score'] if clusters else 0
            }
        except Exception as e:
            return {'error': str(e)}
    
    def run_temporal_experiment(self, periods):
        """Run temporal analyzer with given periods"""
        try:
            analyzer = TemporalAnalyzer()
            stories = analyzer.load_processed_data(self.input_file)
            period_objs = analyzer.create_custom_periods(stories, periods)
            
            # Check if we have data
            if any(p['story_count'] == 0 for p in period_objs):
                return {'error': 'No data in one or more periods'}
            
            period_marker_counts = analyzer.analyze_temporal_markers(period_objs)
            marker_stats = analyzer.calculate_temporal_statistics(period_marker_counts)
            
            # Count significant temporal markers
            significant = [m for m, s in marker_stats.items() if s['max_z_score'] >= 2.0]
            
            return {
                'total_markers': len(marker_stats),
                'significant_markers': len(significant),
                'avg_z_score': sum(s['max_z_score'] for s in marker_stats.values()) / len(marker_stats) if marker_stats else 0,
                'top_marker': max(marker_stats.items(), key=lambda x: x[1]['max_z_score'])[0] if marker_stats else 'None',
                'top_marker_z': max(marker_stats.items(), key=lambda x: x[1]['max_z_score'])[1]['max_z_score'] if marker_stats else 0
            }
        except Exception as e:
            return {'error': str(e)}
    
    def run_experiment(self, exp, event_date):
        """
        Run a complete experiment with all analyzers
        
        Args:
            exp: Experiment configuration
            event_date: Event date
            
        Returns:
            Dictionary with results from all analyzers
        """
        print("\n" + "=" * 120)
        print(f"RUNNING EXPERIMENT: {exp['name']}")
        print(f"Description: {exp['description']}")
        print("=" * 120)
        
        # Calculate periods
        periods = self.calculate_periods_for_experiment(exp, event_date)
        
        print(f"\nDate Ranges:")
        for label, start, end in periods:
            print(f"  {label}: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')} ({(end-start).days} days)")
        
        results = {
            'experiment': exp['name'],
            'description': exp['description'],
            'baseline_days': exp['baseline_days'],
            'monitoring_days': exp['monitoring_days'],
            'event_days': exp['event_days']
        }
        
        # Run word frequency analysis
        print("\n  Running word frequency analysis...")
        results['word_freq'] = self.run_word_frequency_experiment(periods)
        
        # Run semantic analysis
        print("  Running semantic analysis...")
        results['semantic'] = self.run_semantic_experiment(periods)
        
        # Run temporal analysis
        print("  Running temporal analysis...")
        results['temporal'] = self.run_temporal_experiment(periods)
        
        return results
    
    def display_comparison_table(self, all_results):
        """Display comparison table of all experiments"""
        print("\n" + "=" * 120)
        print("EXPERIMENT COMPARISON TABLE")
        print("=" * 120)
        
        # Word Frequency Results
        print("\n" + "-" * 120)
        print("WORD FREQUENCY ANALYSIS")
        print("-" * 120)
        header = f"{'Experiment':<25}{'Days (B-M-E)':<20}{'Total':<10}{'Very Strong':<15}{'Strong':<10}{'Avg Z':<10}{'Top Word':<20}"
        print(header)
        print("-" * 120)
        
        for result in all_results:
            wf = result['word_freq']
            if 'error' in wf:
                print(f"{result['experiment']:<25}{result['baseline_days']}-{result['monitoring_days']}-{result['event_days']:<20}ERROR: {wf['error']}")
            else:
                days = f"{result['baseline_days']}-{result['monitoring_days']}-{result['event_days']}"
                print(f"{result['experiment']:<25}{days:<20}{wf['total_signals']:<10}{wf['very_strong']:<15}{wf['strong']:<10}{wf['avg_z_score']:<10.2f}{wf['top_word']:<20}")
        
        # Semantic Results
        print("\n" + "-" * 120)
        print("SEMANTIC CLUSTER ANALYSIS")
        print("-" * 120)
        header = f"{'Experiment':<25}{'Days (B-M-E)':<20}{'Clusters':<10}{'Avg Size':<12}{'Avg Z':<10}{'Top Cluster':<30}"
        print(header)
        print("-" * 120)
        
        for result in all_results:
            sem = result['semantic']
            if 'error' in sem:
                print(f"{result['experiment']:<25}{result['baseline_days']}-{result['monitoring_days']}-{result['event_days']:<20}ERROR: {sem['error']}")
            else:
                days = f"{result['baseline_days']}-{result['monitoring_days']}-{result['event_days']}"
                top_cluster = sem.get('top_cluster_words', 'None')[:28]
                print(f"{result['experiment']:<25}{days:<20}{sem['total_clusters']:<10}{sem['avg_cluster_size']:<12.1f}{sem['avg_z_score']:<10.2f}{top_cluster:<30}")
        
        # Temporal Results
        print("\n" + "-" * 120)
        print("TEMPORAL MARKER ANALYSIS")
        print("-" * 120)
        header = f"{'Experiment':<25}{'Days (B-M-E)':<20}{'Total':<10}{'Significant':<15}{'Avg Z':<10}{'Top Marker':<20}"
        print(header)
        print("-" * 120)
        
        for result in all_results:
            temp = result['temporal']
            if 'error' in temp:
                print(f"{result['experiment']:<25}{result['baseline_days']}-{result['monitoring_days']}-{result['event_days']:<20}ERROR: {temp['error']}")
            else:
                days = f"{result['baseline_days']}-{result['monitoring_days']}-{result['event_days']}"
                print(f"{result['experiment']:<25}{days:<20}{temp['total_markers']:<10}{temp['significant_markers']:<15}{temp['avg_z_score']:<10.2f}{temp['top_marker']:<20}")
        
        print("=" * 120)
    
    def save_results_to_csv(self, all_results, output_file):
        """Save experiment results to CSV"""
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'experiment', 'description', 'baseline_days', 'monitoring_days', 'event_days',
                'wf_total_signals', 'wf_very_strong', 'wf_strong', 'wf_avg_z',
                'sem_clusters', 'sem_avg_size', 'sem_avg_z',
                'temp_total', 'temp_significant', 'temp_avg_z'
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in all_results:
                wf = result['word_freq']
                sem = result['semantic']
                temp = result['temporal']
                
                row = {
                    'experiment': result['experiment'],
                    'description': result['description'],
                    'baseline_days': result['baseline_days'],
                    'monitoring_days': result['monitoring_days'],
                    'event_days': result['event_days'],
                    'wf_total_signals': wf.get('total_signals', 0) if 'error' not in wf else 'ERROR',
                    'wf_very_strong': wf.get('very_strong', 0) if 'error' not in wf else '',
                    'wf_strong': wf.get('strong', 0) if 'error' not in wf else '',
                    'wf_avg_z': wf.get('avg_z_score', 0) if 'error' not in wf else '',
                    'sem_clusters': sem.get('total_clusters', 0) if 'error' not in sem else 'ERROR',
                    'sem_avg_size': sem.get('avg_cluster_size', 0) if 'error' not in sem else '',
                    'sem_avg_z': sem.get('avg_z_score', 0) if 'error' not in sem else '',
                    'temp_total': temp.get('total_markers', 0) if 'error' not in temp else 'ERROR',
                    'temp_significant': temp.get('significant_markers', 0) if 'error' not in temp else '',
                    'temp_avg_z': temp.get('avg_z_score', 0) if 'error' not in temp else ''
                }
                
                writer.writerow(row)
        
        print(f"\n✅ Results saved to: {output_file}")


# Main execution
if __name__ == "__main__":
    print("=" * 120)
    print("DATE RANGE EXPERIMENT - FINDING OPTIMAL SIGNAL WINDOWS")
    print("=" * 120)
    
    input_file = 'data/processed/hackernews_training_processed.csv'
    
    # Define the known event date (adjust this to your July 2024 event)
    # Using July 1, 2024 as placeholder - you can adjust
    event_date = datetime(2024, 7, 1)
    
    print(f"\nTarget event date: {event_date.strftime('%Y-%m-%d')}")
    print("Testing different date range windows to find optimal signal detection...")
    
    # Initialize experiment
    experiment = DateRangeExperiment(input_file)
    
    # Define experiments
    experiments = experiment.define_experiments(event_date)
    
    print(f"\nRunning {len(experiments)} experiments...")
    print("This may take several minutes...\n")
    
    # Run all experiments
    all_results = []
    for exp in experiments:
        result = experiment.run_experiment(exp, event_date)
        all_results.append(result)
    
    # Display comparison
    experiment.display_comparison_table(all_results)
    
    # Save results
    output_csv = 'data/analysis/date_range_experiments.csv'
    experiment.save_results_to_csv(all_results, output_csv)
    
    print("\n" + "=" * 120)
    print("EXPERIMENT COMPLETE")
    print("=" * 120)
    print(f"\nResults saved to: {output_csv}")
    print("\nRECOMMENDATIONS:")
    print("1. Review the comparison table above")
    print("2. Look for experiments with highest avg z-scores and most significant signals")
    print("3. Balance signal strength vs. practical monitoring frequency")
    print("4. Consider data availability constraints")
    print("\nNEXT: Choose optimal date range and re-run detailed analysis!")
    print("=" * 120)