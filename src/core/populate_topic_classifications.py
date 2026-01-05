"""
populate_topic_classifications.py - Iterative LLM-based topic classification

Uses Claude API to classify stories into topics with iterative refinement
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


class TopicClassifier:
    """Iterative topic classification with LLM"""
    
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
    
    def __init__(self, db_path: str = "data/linguistic_predictor.db"):
        """Initialize classifier"""
        self.db = Database(db_path)
        self.current_topics = self.SEED_TOPICS.copy()
        
        # Check if we can use Claude API
        try:
            import anthropic
            self.anthropic_available = True
            print("✓ Anthropic API available")
        except ImportError:
            self.anthropic_available = False
            print("⚠️  Anthropic library not installed. Install with: pip install anthropic")
    
    def classify_story(self, story_text: str, topics: List[str]) -> Dict:
        """
        Classify a single story using Claude API
        
        Args:
            story_text: Story title/text
            topics: List of allowed topics
            
        Returns:
            Dictionary with topic and confidence
        """
        if not self.anthropic_available:
            return {'topic': 'other', 'confidence': 0.0}
        
        # Build prompt
        topics_list = '\n'.join([f"- {topic}" for topic in topics])
        
        prompt = f"""Analyze this text and classify the PRIMARY topic of tension or concern.

You must choose EXACTLY ONE from this list:
{topics_list}

Text: "{story_text}"

Reply with ONLY the topic label from the list above, nothing else. No explanation, no punctuation, just the exact topic name."""
        
        try:
            import anthropic
            client = anthropic.Anthropic()
            
            message = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=50,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            topic = message.content[0].text.strip().lower()
            
            # Validate topic is in allowed list
            if topic not in topics:
                # Try to find closest match
                for allowed_topic in topics:
                    if allowed_topic in topic or topic in allowed_topic:
                        topic = allowed_topic
                        break
                else:
                    topic = 'other'
            
            # Rough confidence based on response (could be improved)
            confidence = 0.8  # Default for now
            
            return {'topic': topic, 'confidence': confidence}
            
        except Exception as e:
            print(f"    Error classifying: {e}")
            return {'topic': 'other', 'confidence': 0.0}
    
    def classify_batch(self, iteration: int, batch_size: int = 100, 
                      sample_size: int = None):
        """
        Classify a batch of stories for a specific iteration
        
        Args:
            iteration: Which iteration (1-4)
            batch_size: Stories to process before committing
            sample_size: If set, only classify this many stories (for testing)
        """
        print(f"\n{'='*80}")
        print(f"CLASSIFYING ITERATION {iteration}")
        print(f"{'='*80}")
        print(f"Topics: {', '.join(self.current_topics)}")
        
        # Get stories that don't have this iteration classified yet
        iteration_col = f"iteration_{iteration}"
        
        query = f"""
            SELECT s.id, s.title
            FROM stories s
            LEFT JOIN topic_classifications tc ON s.id = tc.story_id
            WHERE tc.{iteration_col} IS NULL
            AND s.content_type = 'header'
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
            result = self.classify_story(title, self.current_topics)
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
                
                # Rate limiting - be nice to API
                time.sleep(1)
        
        # Final commit
        self.db.conn.commit()
        
        print(f"\n✓ Classified {processed} stories")
        print(f"\nTopic distribution:")
        for topic, count in topic_counts.most_common():
            pct = (count / processed) * 100
            print(f"  {topic:25s}: {count:5d} ({pct:5.1f}%)")
        
        print("="*80)
    
    def suggest_new_topics(self, iteration: int, num_suggestions: int = 5) -> List[str]:
        """
        Ask Claude to suggest new topics based on 'other' classifications
        
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
            LIMIT 50
        """
        
        self.db.cursor.execute(query)
        other_stories = [row[0] for row in self.db.cursor.fetchall()]
        
        if len(other_stories) < 5:
            print("Not enough 'other' stories to suggest new topics")
            return []
        
        print(f"Analyzing {len(other_stories)} 'other' stories...")
        
        # Build prompt
        stories_sample = '\n'.join([f"- {story}" for story in other_stories[:30]])
        current_topics_list = '\n'.join([f"- {topic}" for topic in self.current_topics])
        
        prompt = f"""Looking at these story titles that were classified as "other", suggest {num_suggestions} new specific topic categories that would better capture these stories.

Current topics we already have:
{current_topics_list}

Stories classified as "other":
{stories_sample}

Reply with ONLY a JSON array of {num_suggestions} new topic names (lowercase, underscore_separated). No explanation.

Example format: ["crypto_blockchain", "space_exploration", "ai_regulation"]"""
        
        try:
            import anthropic
            client = anthropic.Anthropic()
            
            message = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=200,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            response = message.content[0].text.strip()
            
            # Parse JSON response
            suggestions = json.loads(response)
            
            print(f"\nSuggested new topics:")
            for topic in suggestions:
                print(f"  + {topic}")
            
            return suggestions
            
        except Exception as e:
            print(f"Error getting suggestions: {e}")
            return []
    
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
                WHERE iteration_{i} IS NOT NULL
            """)
            count = self.db.cursor.fetchone()[0]
            print(f"Iteration {i}: {count} stories classified")
        
        # Stability analysis
        self.db.cursor.execute("""
            SELECT 
                COUNT(CASE WHEN iteration_1 = iteration_2 THEN 1 END) * 100.0 / COUNT(*) as stable_1_2,
                COUNT(CASE WHEN iteration_2 = iteration_3 THEN 1 END) * 100.0 / COUNT(*) as stable_2_3,
                COUNT(CASE WHEN iteration_3 = iteration_4 THEN 1 END) * 100.0 / COUNT(*) as stable_3_4
            FROM topic_classifications
            WHERE iteration_2 IS NOT NULL
        """)
        
        stability = self.db.cursor.fetchone()
        if stability[0] is not None:
            print(f"\nStability between iterations:")
            print(f"  1→2: {stability[0]:.1f}% unchanged")
            if stability[1] is not None:
                print(f"  2→3: {stability[1]:.1f}% unchanged")
            if stability[2] is not None:
                print(f"  3→4: {stability[2]:.1f}% unchanged")
        
        print("="*80)
    
    def close(self):
        """Close database connection"""
        self.db.close()


# Main execution
if __name__ == "__main__":
    import sys
    
    # Parse arguments
    sample_size = None
    if len(sys.argv) > 1:
        try:
            sample_size = int(sys.argv[1])
            print(f"Running with sample size: {sample_size}")
        except:
            print("Usage: python3 -m src.core.populate_topic_classifications [sample_size]")
            print("\nExample: python3 -m src.core.populate_topic_classifications 1000")
            sys.exit(1)
    
    print("="*80)
    print("ITERATIVE TOPIC CLASSIFICATION")
    print("="*80)
    
    classifier = TopicClassifier()
    
    # Run all 4 iterations
    for iteration in range(1, 5):
        classifier.run_iteration(iteration, sample_size=sample_size)
        
        # Small delay between iterations
        if iteration < 4:
            print("\nWaiting 5 seconds before next iteration...")
            time.sleep(5)
    
    # Show final statistics
    classifier.show_final_stats()
    
    classifier.close()
    
    print("\n✅ Topic classification complete!")