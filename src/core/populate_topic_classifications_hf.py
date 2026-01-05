"""
populate_topic_classifications_hf.py - Topic classification with Hugging Face

Uses free Hugging Face models for iterative topic classification
Modular design allows swapping to paid APIs later
"""

import sys
import os
import json
import time
from datetime import datetime
from collections import Counter
from typing import List, Dict, Set

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.database import Database


class LLMProvider:
    """Base class for LLM providers"""
    
    def classify(self, text: str, topics: List[str]) -> Dict:
        """Classify text into one of the given topics"""
        raise NotImplementedError
    
    def suggest_topics(self, texts: List[str], current_topics: List[str], 
                      num_suggestions: int) -> List[str]:
        """Suggest new topics based on unclassified texts"""
        raise NotImplementedError


class HuggingFaceProvider(LLMProvider):
    """Hugging Face implementation (free)"""
    
    def __init__(self):
        """Initialize Hugging Face client"""
        try:
            from transformers import pipeline
            print("✓ Hugging Face transformers available")
            
            # Use zero-shot classification model (works without fine-tuning)
            print("Loading zero-shot classification model...")
            self.classifier = pipeline(
                "zero-shot-classification",
                model="facebook/bart-large-mnli",
                device=-1  # CPU (use 0 for GPU if available)
            )
            print("✓ Model loaded")
            
        except ImportError:
            print("⚠️  Hugging Face transformers not installed")
            print("   Install with: pip install transformers torch")
            self.classifier = None
    
    def classify(self, text: str, topics: List[str]) -> Dict:
        """
        Classify text using zero-shot classification
        
        Args:
            text: Text to classify
            topics: List of possible topics
            
        Returns:
            Dictionary with topic and confidence
        """
        if not self.classifier:
            return {'topic': 'other', 'confidence': 0.0}
        
        if not text or not topics:
            return {'topic': 'other', 'confidence': 0.0}
        
        try:
            # Truncate text if too long (model has limits)
            text = text[:512]
            
            # Run classification
            result = self.classifier(
                text,
                candidate_labels=topics,
                multi_label=False
            )
            
            # Get top prediction
            topic = result['labels'][0]
            confidence = result['scores'][0]
            
            return {
                'topic': topic,
                'confidence': round(confidence, 3)
            }
            
        except Exception as e:
            print(f"    Classification error: {e}")
            return {'topic': 'other', 'confidence': 0.0}
    
    def suggest_topics(self, texts: List[str], current_topics: List[str],
                      num_suggestions: int = 5) -> List[str]:
        """
        Suggest new topics by analyzing common words in unclassified texts
        
        This is a simple keyword extraction approach
        For better results, could use a paid API here
        
        Args:
            texts: Sample of unclassified texts
            current_topics: Topics we already have
            num_suggestions: How many to suggest
            
        Returns:
            List of suggested topic names
        """
        print(f"  Analyzing {len(texts)} texts for new topics...")
        
        # Extract common meaningful words
        from collections import Counter
        import re
        
        # Combine all texts
        combined = ' '.join(texts).lower()
        
        # Extract words (3+ chars, alphabetic)
        words = re.findall(r'\b[a-z]{3,}\b', combined)
        
        # Remove stopwords
        stopwords = {'the', 'and', 'for', 'that', 'this', 'with', 'from', 
                    'are', 'was', 'has', 'have', 'been', 'will', 'can', 
                    'but', 'not', 'what', 'all', 'were', 'when', 'your',
                    'how', 'they', 'more', 'than', 'about', 'into', 'after',
                    'other', 'some', 'could', 'would', 'should', 'their'}
        
        words = [w for w in words if w not in stopwords]
        
        # Get most common
        word_counts = Counter(words)
        top_words = word_counts.most_common(20)
        
        # Group related words into topic suggestions
        # This is simplistic - a real LLM would do better
        suggestions = []
        
        # Look for topic clusters
        topic_keywords = {
            'crypto_blockchain': ['crypto', 'bitcoin', 'blockchain', 'ethereum'],
            'ai_ml': ['model', 'training', 'neural', 'algorithm', 'llm'],
            'space_exploration': ['space', 'nasa', 'rocket', 'mars', 'satellite'],
            'cybersecurity': ['security', 'hack', 'breach', 'vulnerability', 'attack'],
            'energy': ['energy', 'solar', 'nuclear', 'battery', 'power'],
            'education': ['school', 'student', 'university', 'learning', 'education'],
            'entertainment': ['game', 'video', 'movie', 'music', 'show'],
            'transportation': ['car', 'vehicle', 'transport', 'traffic', 'road'],
            'real_estate': ['housing', 'property', 'rent', 'home', 'real'],
            'labor_employment': ['job', 'work', 'employee', 'labor', 'unemployment']
        }
        
        # Check which topic clusters appear frequently
        for topic, keywords in topic_keywords.items():
            if topic in current_topics:
                continue
            
            # Count how many of these keywords appear
            keyword_count = sum(word_counts.get(kw, 0) for kw in keywords)
            
            if keyword_count > 5:  # Threshold
                suggestions.append(topic)
                
                if len(suggestions) >= num_suggestions:
                    break
        
        # If we didn't find enough, suggest based on raw frequency
        if len(suggestions) < num_suggestions:
            for word, count in top_words:
                if count > 10 and word not in current_topics:
                    topic_name = f"{word}_related"
                    if topic_name not in suggestions:
                        suggestions.append(topic_name)
                    
                    if len(suggestions) >= num_suggestions:
                        break
        
        return suggestions[:num_suggestions]


class GroqProvider(LLMProvider):
    """Groq implementation (paid but cheap) - placeholder for future"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        print("⚠️  Groq provider not implemented yet")
    
    def classify(self, text: str, topics: List[str]) -> Dict:
        return {'topic': 'other', 'confidence': 0.0}
    
    def suggest_topics(self, texts: List[str], current_topics: List[str],
                      num_suggestions: int) -> List[str]:
        return []


class TopicClassifier:
    """Iterative topic classification with pluggable LLM providers"""
    
    # Seed topics for iteration 1
    SEED_TOPICS = [
        'politics',
        'finance_markets',
        'technology',
        'security_defense',
        'healthcare',
        'climate_environment',
        'corporate_business',
        'legal_regulatory',
        'international_conflict',
        'social_culture',
        'other'
    ]
    
    def __init__(self, db_path: str = "data/linguistic_predictor.db",
                 provider: str = 'huggingface'):
        """
        Initialize classifier
        
        Args:
            db_path: Path to database
            provider: 'huggingface', 'groq', or 'grok'
        """
        self.db = Database(db_path)
        self.current_topics = self.SEED_TOPICS.copy()
        
        # Initialize provider
        if provider == 'huggingface':
            self.provider = HuggingFaceProvider()
        elif provider == 'groq':
            self.provider = GroqProvider(api_key=os.getenv('GROQ_API_KEY'))
        else:
            raise ValueError(f"Unknown provider: {provider}")
        
        self.provider_name = provider
    
    def classify_batch(self, iteration: int, batch_size: int = 100,
                      sample_size: int = None):
        """
        Classify a batch of stories for a specific iteration
        
        Args:
            iteration: Which iteration (1-4)
            batch_size: Stories to process before committing
            sample_size: If set, only classify this many stories
        """
        print(f"\n{'='*80}")
        print(f"CLASSIFYING ITERATION {iteration}")
        print(f"{'='*80}")
        print(f"Provider: {self.provider_name}")
        print(f"Topics: {', '.join(self.current_topics)}")
        
        # Get stories that don't have this iteration classified yet
        iteration_col = f"iteration_{iteration}"
        
        query = f"""
            SELECT s.id, s.title
            FROM stories s
            LEFT JOIN topic_classifications tc ON s.id = tc.story_id
            WHERE (tc.{iteration_col} IS NULL OR tc.{iteration_col} = '')
            AND s.content_type = 'header'
            AND s.title IS NOT NULL
            AND s.title != ''
        """
        
        if sample_size:
            query += f" LIMIT {sample_size}"
        
        self.db.cursor.execute(query)
        stories = self.db.cursor.fetchall()
        
        total = len(stories)
        print(f"Stories to classify: {total}")
        
        if total == 0:
            print(f"All stories already classified for iteration {iteration}!")
            return
        
        processed = 0
        topic_counts = Counter()
        
        for i, (story_id, title) in enumerate(stories, 1):
            # Classify
            result = self.provider.classify(title, self.current_topics)
            topic = result['topic']
            confidence = result['confidence']
            
            topic_counts[topic] += 1
            
            # Upsert into database
            self.db.cursor.execute(f"""
                INSERT INTO topic_classifications 
                    (story_id, {iteration_col}, confidence_{iteration}, classified_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(story_id) DO UPDATE SET
                    {iteration_col} = excluded.{iteration_col},
                    confidence_{iteration} = excluded.confidence_{iteration},
                    classified_at = excluded.classified_at
            """, (
                story_id,
                topic,
                confidence,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
            
            processed += 1
            
            # Commit in batches
            if processed % batch_size == 0:
                self.db.conn.commit()
                progress = (processed / total) * 100
                print(f"  Progress: {progress:.1f}% ({processed}/{total})", end='\r')
        
        # Final commit
        self.db.conn.commit()
        
        print(f"\n✓ Classified {processed} stories")
        print(f"\nTopic distribution:")
        for topic, count in topic_counts.most_common():
            pct = (count / processed) * 100
            print(f"  {topic:30s}: {count:5d} ({pct:5.1f}%)")
        
        print("="*80)
    
    def suggest_new_topics(self, iteration: int, num_suggestions: int = 5) -> List[str]:
        """
        Suggest new topics based on 'other' classifications
        
        Args:
            iteration: Which iteration we just completed
            num_suggestions: How many new topics to suggest
            
        Returns:
            List of suggested topic names
        """
        print(f"\n{'='*80}")
        print(f"SUGGESTING NEW TOPICS (after iteration {iteration})")
        print(f"{'='*80}")
        
        # Get sample of stories classified as 'other'
        iteration_col = f"iteration_{iteration}"
        
        query = f"""
            SELECT s.title
            FROM stories s
            JOIN topic_classifications tc ON s.id = tc.story_id
            WHERE tc.{iteration_col} = 'other'
            LIMIT 100
        """
        
        self.db.cursor.execute(query)
        other_stories = [row[0] for row in self.db.cursor.fetchall()]
        
        if len(other_stories) < 5:
            print("Not enough 'other' stories to suggest new topics")
            return []
        
        print(f"Analyzing {len(other_stories)} 'other' stories...")
        
        # Use provider to suggest topics
        suggestions = self.provider.suggest_topics(
            other_stories,
            self.current_topics,
            num_suggestions
        )
        
        if suggestions:
            print(f"\nSuggested new topics:")
            for topic in suggestions:
                print(f"  + {topic}")
        else:
            print("\nNo new topics suggested")
        
        return suggestions
    
    def run_iteration(self, iteration: int, sample_size: int = None):
        """
        Run one complete iteration: classify → suggest → update topics
        
        Args:
            iteration: Which iteration (1-4)
            sample_size: If set, only process this many stories
        """
        print(f"\n{'#'*80}")
        print(f"# ITERATION {iteration}")
        print(f"{'#'*80}")
        
        # Classify with current topics
        self.classify_batch(iteration, sample_size=sample_size)
        
        # After iterations 1-3, suggest new topics
        if iteration < 4:
            suggestions = self.suggest_new_topics(iteration)
            
            if suggestions:
                print(f"\nAdding {len(suggestions)} new topics to taxonomy")
                self.current_topics.extend(suggestions)
                print(f"Total topics now: {len(self.current_topics)}")
            else:
                print("\nNo new topics suggested, continuing with current taxonomy")
    
    def show_final_stats(self):
        """Show final classification statistics"""
        print(f"\n{'='*80}")
        print("FINAL CLASSIFICATION STATISTICS")
        print(f"{'='*80}")
        
        # Count classifications per iteration
        for i in range(1, 5):
            self.db.cursor.execute(f"""
                SELECT COUNT(*) 
                FROM topic_classifications 
                WHERE iteration_{i} IS NOT NULL AND iteration_{i} != ''
            """)
            count = self.db.cursor.fetchone()[0]
            print(f"Iteration {i}: {count} stories classified")
        
        # Stability analysis
        self.db.cursor.execute("""
            SELECT 
                COUNT(CASE WHEN iteration_1 = iteration_2 THEN 1 END) * 100.0 / 
                    NULLIF(COUNT(CASE WHEN iteration_2 IS NOT NULL AND iteration_2 != '' THEN 1 END), 0) as stable_1_2,
                COUNT(CASE WHEN iteration_2 = iteration_3 THEN 1 END) * 100.0 / 
                    NULLIF(COUNT(CASE WHEN iteration_3 IS NOT NULL AND iteration_3 != '' THEN 1 END), 0) as stable_2_3,
                COUNT(CASE WHEN iteration_3 = iteration_4 THEN 1 END) * 100.0 / 
                    NULLIF(COUNT(CASE WHEN iteration_4 IS NOT NULL AND iteration_4 != '' THEN 1 END), 0) as stable_3_4
            FROM topic_classifications
            WHERE iteration_2 IS NOT NULL AND iteration_2 != ''
        """)
        
        stability = self.db.cursor.fetchone()
        if stability[0] is not None:
            print(f"\nStability between iterations:")
            print(f"  1→2: {stability[0]:.1f}% unchanged")
            if stability[1] is not None:
                print(f"  2→3: {stability[1]:.1f}% unchanged")
            if stability[2] is not None:
                print(f"  3→4: {stability[2]:.1f}% unchanged")
        
        # Final topic distribution
        self.db.cursor.execute("""
            SELECT iteration_4, COUNT(*) as count
            FROM topic_classifications
            WHERE iteration_4 IS NOT NULL AND iteration_4 != ''
            GROUP BY iteration_4
            ORDER BY count DESC
            LIMIT 15
        """)
        
        print(f"\nFinal topic distribution (top 15):")
        for row in self.db.cursor.fetchall():
            print(f"  {row[0]:30s}: {row[1]:5d}")
        
        print("="*80)
    
    def close(self):
        """Close database connection"""
        self.db.close()


# Main execution
if __name__ == "__main__":
    import sys
    
    # Parse arguments
    sample_size = None
    provider = 'huggingface'  # Default
    
    if len(sys.argv) > 1:
        try:
            sample_size = int(sys.argv[1])
            print(f"Running with sample size: {sample_size}")
        except:
            print("Usage: python3 -m src.core.populate_topic_classifications_hf [sample_size] [provider]")
            print("\nExample: python3 -m src.core.populate_topic_classifications_hf 1000 huggingface")
            sys.exit(1)
    
    if len(sys.argv) > 2:
        provider = sys.argv[2]
    
    print("="*80)
    print("ITERATIVE TOPIC CLASSIFICATION")
    print(f"Provider: {provider}")
    print("="*80)
    
    classifier = TopicClassifier(provider=provider)
    
    # Run all 4 iterations
    for iteration in range(1, 5):
        classifier.run_iteration(iteration, sample_size=sample_size)
        
        # Small delay between iterations
        if iteration < 4:
            print("\nWaiting 3 seconds before next iteration...")
            time.sleep(3)
    
    # Show final statistics
    classifier.show_final_stats()
    
    classifier.close()
    
    print("\n✅ Topic classification complete!")