# topic_analyzer.py - Topic modeling for predictive linguistic analysis
# Phase 2: Topic Modeling (NMF)

import csv
from collections import Counter, defaultdict
from datetime import datetime
import statistics
import os
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF
import numpy as np

class TopicAnalyzer:
    """
    Analyzes topics using NMF and tracks their intensity changes over time
    """
    
    def __init__(self, n_topics=12, top_words=10):
        """
        Initialize the analyzer
        
        Args:
            n_topics: Number of topics to extract
            top_words: Number of top words to display per topic
        """
        self.n_topics = n_topics
        self.top_words = top_words
        print(f"Topic Analyzer initialized (NMF with {n_topics} topics)")
    
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
    
    def build_topic_model(self, all_stories):
        """
        Build NMF topic model from all stories
        
        Args:
            all_stories: List of all story dictionaries
            
        Returns:
            Tuple of (model, vectorizer, feature_names)
        """
        print("\n" + "=" * 120)
        print("BUILDING TOPIC MODEL")
        print("=" * 120)
        
        # Prepare documents (use processed words)
        documents = []
        for story in all_stories:
            words = story['words'].split('|') if story['words'] else []
            # Reconstruct as space-separated text for TF-IDF
            documents.append(' '.join(words))
        
        print(f"Processing {len(documents)} documents...")
        
        # Create TF-IDF matrix
        vectorizer = TfidfVectorizer(
            max_features=1000,      # Top 1000 words
            min_df=3,               # Word must appear in at least 3 documents
            max_df=0.7,             # Ignore words in more than 70% of docs
            ngram_range=(1, 2)      # Include single words and bigrams
        )
        
        tfidf_matrix = vectorizer.fit_transform(documents)
        feature_names = vectorizer.get_feature_names_out()
        
        print(f"TF-IDF matrix shape: {tfidf_matrix.shape}")
        print(f"Vocabulary size: {len(feature_names)}")
        
        # Build NMF model
        print(f"\nTraining NMF model with {self.n_topics} topics...")
        nmf_model = NMF(
            n_components=self.n_topics,
            random_state=42,
            max_iter=200,
            init='nndsvda'
        )
        
        nmf_model.fit(tfidf_matrix)
        
        print("✓ Topic model built successfully")
        
        return nmf_model, vectorizer, feature_names
    
    def display_topics(self, model, feature_names):
        """Display the extracted topics"""
        print("\n" + "=" * 120)
        print(f"EXTRACTED TOPICS (Top {self.top_words} words per topic)")
        print("=" * 120)
        
        topics_data = []
        
        for topic_idx, topic in enumerate(model.components_):
            print(f"\nTopic {topic_idx + 1}:")
            top_indices = topic.argsort()[-self.top_words:][::-1]
            top_words = [feature_names[i] for i in top_indices]
            top_weights = [topic[i] for i in top_indices]
            
            # Display words with weights
            words_str = ", ".join([f"{word}({weight:.2f})" for word, weight in zip(top_words, top_weights)])
            print(f"  {words_str}")
            
            topics_data.append({
                'topic_id': topic_idx + 1,
                'words': top_words,
                'weights': top_weights
            })
        
        return topics_data
    
    def calculate_topic_intensity(self, period, model, vectorizer):
        """
        Calculate topic intensity (document-topic distribution) for a period
        
        Args:
            period: Period dictionary
            model: Trained NMF model
            vectorizer: TF-IDF vectorizer
            
        Returns:
            Array of topic intensities (sum of topic weights across all documents)
        """
        # Prepare documents
        documents = []
        for story in period['stories']:
            words = story['words'].split('|') if story['words'] else []
            documents.append(' '.join(words))
        
        if not documents:
            return np.zeros(self.n_topics)
        
        # Transform to TF-IDF
        tfidf_matrix = vectorizer.transform(documents)
        
        # Get document-topic distribution
        doc_topic_dist = model.transform(tfidf_matrix)
        
        # Sum topic intensities across all documents
        topic_intensities = doc_topic_dist.sum(axis=0)
        
        return topic_intensities
    
    def analyze_topic_changes(self, periods, all_stories):
        """
        Analyze how topic intensities change across periods
        
        Args:
            periods: List of period dictionaries
            all_stories: All stories for building model
            
        Returns:
            Dictionary with topic analysis results
        """
        print("\n" + "=" * 120)
        print("ANALYZING TOPIC INTENSITY CHANGES")
        print("=" * 120)
        
        # Build topic model on all data
        model, vectorizer, feature_names = self.build_topic_model(all_stories)
        
        # Display topics
        topics_data = self.display_topics(model, feature_names)
        
        # Calculate topic intensities for each period
        print("\n" + "=" * 120)
        print("TOPIC INTENSITIES BY PERIOD")
        print("=" * 120)
        
        period_intensities = []
        for period in periods:
            intensities = self.calculate_topic_intensity(period, model, vectorizer)
            period_intensities.append(intensities)
            print(f"{period['label']}: {period['story_count']} stories")
        
        # Calculate statistics (z-scores, velocity, acceleration)
        topic_stats = []
        
        for topic_idx in range(self.n_topics):
            # Get intensities across periods
            intensities = [pi[topic_idx] for pi in period_intensities]
            
            # Baseline (first period)
            baseline = intensities[0]
            baseline_mean = baseline
            
            # For single baseline, estimate stdev
            baseline_stdev = baseline * 0.3 if baseline > 0 else 1.0
            
            # Calculate z-scores for subsequent periods
            z_scores = []
            for i in range(1, len(intensities)):
                if baseline_stdev > 0:
                    z = (intensities[i] - baseline_mean) / baseline_stdev
                else:
                    z = 10.0 if intensities[i] > baseline_mean else 0.0
                z_scores.append(z)
            
            # Velocity (change between periods)
            velocities = []
            for i in range(1, len(intensities)):
                vel = intensities[i] - intensities[i-1]
                velocities.append(vel)
            
            # Acceleration
            if len(velocities) >= 2:
                acceleration = velocities[-1] - velocities[-2]
            else:
                acceleration = 0
            
            topic_stats.append({
                'topic_id': topic_idx + 1,
                'topic_words': topics_data[topic_idx]['words'],
                'intensities': intensities,
                'baseline_mean': baseline_mean,
                'baseline_stdev': baseline_stdev,
                'z_scores': z_scores,
                'max_z_score': max(z_scores) if z_scores else 0,
                'velocity': velocities[-1] if velocities else 0,
                'acceleration': acceleration
            })
        
        return topic_stats, topics_data
    
    def display_topic_analysis(self, topic_stats, periods, top_n=10):
        """Display topic analysis results"""
        # Sort by max z-score
        sorted_stats = sorted(topic_stats, key=lambda x: x['max_z_score'], reverse=True)
        
        print("\n" + "=" * 120)
        print(f"TOP {top_n} TOPICS BY Z-SCORE (Strongest Emerging Themes)")
        print("=" * 120)
        
        # Header
        header = f"{'Topic':<8}{'Top Words':<50}"
        for period in periods:
            header += f"{period['label'][:10]:>12}"
        header += f"{'Max Z':>10}{'Velocity':>12}{'Accel':>10}{'Signal':>15}"
        
        print(header)
        print("-" * 120)
        
        # Display top topics
        for stat in sorted_stats[:top_n]:
            # Topic label
            topic_label = f"Topic {stat['topic_id']}"
            
            # Top 5 words
            top_words = ', '.join(stat['topic_words'][:5])
            
            row = f"{topic_label:<8}{top_words:<50}"
            
            # Intensities
            for intensity in stat['intensities']:
                row += f"{intensity:>12.1f}"
            
            # Stats
            row += f"{stat['max_z_score']:>10.2f}"
            row += f"{stat['velocity']:>12.1f}"
            row += f"{stat['acceleration']:>10.1f}"
            
            # Signal strength
            z = stat['max_z_score']
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
    
    def save_topic_analysis_to_csv(self, topic_stats, topics_data, periods, output_file):
        """Save topic analysis to CSV"""
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['topic_id', 'topic_words', 'baseline_intensity']
            
            # Add period columns
            for period in periods:
                fieldnames.append(period['label'])
            
            fieldnames.extend(['max_z_score', 'velocity', 'acceleration', 'signal_strength'])
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for stat in topic_stats:
                row = {
                    'topic_id': stat['topic_id'],
                    'topic_words': ', '.join(stat['topic_words'][:10]),
                    'baseline_intensity': round(stat['baseline_mean'], 2),
                    'max_z_score': round(stat['max_z_score'], 2),
                    'velocity': round(stat['velocity'], 2),
                    'acceleration': round(stat['acceleration'], 2)
                }
                
                # Add period intensities
                for i, period in enumerate(periods):
                    row[period['label']] = round(stat['intensities'][i], 2)
                
                # Signal strength
                z = stat['max_z_score']
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
        
        print(f"\n✅ Topic analysis saved to: {output_file}")


# Main execution
if __name__ == "__main__":
    print("=" * 120)
    print("TOPIC MODELING ANALYZER (NMF)")
    print("=" * 120)
    
    input_file = 'data/processed/hackernews_training_processed.csv'
    
    analyzer = TopicAnalyzer(n_topics=12, top_words=10)
    
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
    
    # Analyze topics
    topic_stats, topics_data = analyzer.analyze_topic_changes(periods, stories)
    
    # Display results
    analyzer.display_topic_analysis(topic_stats, periods, top_n=12)
    
    # Save to CSV
    output_csv = 'data/analysis/topics_may_june_july_2024.csv'
    analyzer.save_topic_analysis_to_csv(topic_stats, topics_data, periods, output_csv)
    
    print("\n" + "=" * 120)
    print("Done!")
    print(f"Source file: {input_file}")
    print(f"Output CSV: {output_csv}")
    print(f"Topics extracted: {analyzer.n_topics}")
    print(f"Date ranges: {', '.join([p['label'] for p in periods])}")
    print("\nNEXT: Review topic themes in CSV and compare to July 2024 events!")
    print("=" * 120)