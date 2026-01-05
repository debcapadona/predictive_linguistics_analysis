"""
topic_classifier.py - Multi-Dimensional Topic Classification

Uses LLM to classify stories across 3 independent dimensions (triangulation):
- Domain: What area/field
- Threat Level: How serious
- Target: Who/what is affected

Provides precise classification through dimensional intersection
"""

from dotenv import load_dotenv
import os

load_dotenv()  # ← this line is the one everyone forgets
api_key = os.getenv("GROQ_API_KEY")

if not api_key:
    raise ValueError("💔 GROQ_API_KEY not found! Check your .env file, gorgeous.")

print(f"Zach is locked and loaded 😈")  # you’ll see this = success

import sys
import os
import csv
from datetime import datetime
from typing import Dict, List
from collections import Counter


# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.database import Database


class TopicClassifier:
    """3-Dimensional topic classification using LLM"""
    
    # Classification dimensions
    DOMAINS = [
        # Tech specifics (HN bread and butter)
        'ai_ml', 'crypto_blockchain', 'programming', 'databases',
        'cybersecurity', 'devops_cloud', 'web_frameworks', 'mobile_dev',
        'gaming', 'hardware', 'open_source',
        
        # Startup/Business
        'startups', 'venture_capital', 'acquisitions', 'layoffs',
        'product_launches', 'saas',
        
        # Science/Research
        'space', 'biotech', 'physics', 'mathematics',
        
        # Policy/Society  
        'tech_regulation', 'privacy_surveillance', 'labor_tech',
        'internet_culture',
        
        # Traditional
        'finance_markets', 'politics', 'education', 'other'
    ]
    
    THREAT_LEVELS = [
        'none',         # Normal news, no problem
        'concern',      # Minor issue, uncertainty
        'crisis',       # Major problem, urgent
        'catastrophe'   # Existential, disaster
    ]
    
    TARGETS = [
        'individual',      # Specific person
        'company',         # Single organization
        'industry',        # Entire sector
        'government',      # Political entity
        'society',         # General public
        'infrastructure',  # Systems/networks
        'nature'          # Environment/animals
    ]
    
def __init__(self, db_path: str = "data/linguistic_predictor.db",
                 provider: str = 'groq'):
        """
        Initialize topic classifier
        
        Args:
            db_path: Path to database
            provider: 'groq' or 'huggingface'
        """
        self.db = Database(db_path)
        self.provider = provider
        self.available = False
        
        if provider == 'groq':
            try:
                from groq import Groq
                api_key = os.getenv('GROQ_API_KEY')
                if api_key:
                    self.client = Groq(api_key=api_key)
                    self.model = "llama-3.1-8b-instant"
                    self.available = True
                    print("✓ Groq API available for topic classification")
                else:
                    print("⚠️  GROQ_API_KEY not set")
            except ImportError:
                print("⚠️  Groq not installed. Install with: pip install groq")
        
        elif provider == 'huggingface':
            try:
                from transformers import pipeline
                print("Loading HuggingFace zero-shot classifier...")
                self.classifier = pipeline(
                    "zero-shot-classification",
                    model="facebook/bart-large-mnli",
                    device=-1  # CPU
                )
                self.available = True
                print("✓ HuggingFace classifier loaded (FREE!)")
            except ImportError:
                print("⚠️  transformers not installed. Install with: pip install transformers torch")
                self.available = False
        
        # Create table
        self._create_table()
    
    def _create_table(self):
        """Create topics table if it doesn't exist"""
        self.db.cursor.execute("""
            CREATE TABLE IF NOT EXISTS story_topics (
                story_id TEXT PRIMARY KEY,
                domain TEXT,
                threat_level TEXT,
                target TEXT,
                domain_confidence REAL,
                threat_confidence REAL,
                target_confidence REAL,
                classified_at TIMESTAMP,
                FOREIGN KEY (story_id) REFERENCES stories(id)
            )
        """)
        
        self.db.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_topics_domain 
            ON story_topics(domain)
        """)
        
        self.db.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_topics_threat 
            ON story_topics(threat_level)
        """)
        
        self.db.conn.commit()
        print("✓ story_topics table ready")
    
    def classify(self, title: str) -> Dict:
        """
        Classify story across 3 dimensions
        
        Args:
            title: Story title
            
        Returns:
            Dictionary with classifications and confidences
        """
        if not self.available or not title:
            return {
                'domain': 'other',
                'threat_level': 'none',
                'target': 'society',
                'domain_confidence': 0.0,
                'threat_confidence': 0.0,
                'target_confidence': 0.0
            }
        
        # Construct prompt
        prompt = f"""Classify this news headline across THREE independent dimensions:

HEADLINE: "{title}"

DIMENSION 1 - DOMAIN (what field/area):
Choose ONE: {', '.join(self.DOMAINS)}

DIMENSION 2 - THREAT LEVEL (how serious):
Choose ONE: {', '.join(self.THREAT_LEVELS)}
- none: Normal news, no problem
- concern: Minor issue, uncertainty, potential problem
- crisis: Major problem, urgent situation
- catastrophe: Disaster, existential threat

DIMENSION 3 - TARGET (who/what affected):
Choose ONE: {', '.join(self.TARGETS)}

Respond in this EXACT format:
DOMAIN: [your choice]
THREAT: [your choice]
TARGET: [your choice]
CONFIDENCE_DOMAIN: [0.0-1.0]
CONFIDENCE_THREAT: [0.0-1.0]
CONFIDENCE_TARGET: [0.0-1.0]"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=150
            )
            
            result = response.choices[0].message.content
            
            # Parse response
            domain = 'other'
            threat = 'none'
            target = 'society'
            conf_domain = 0.5
            conf_threat = 0.5
            conf_target = 0.5
            
            for line in result.split('\n'):
                line = line.strip()
                if line.startswith('DOMAIN:'):
                    domain = line.split(':', 1)[1].strip().lower()
                elif line.startswith('THREAT:'):
                    threat = line.split(':', 1)[1].strip().lower()
                elif line.startswith('TARGET:'):
                    target = line.split(':', 1)[1].strip().lower()
                elif line.startswith('CONFIDENCE_DOMAIN:'):
                    try:
                        conf_domain = float(line.split(':', 1)[1].strip())
                    except:
                        pass
                elif line.startswith('CONFIDENCE_THREAT:'):
                    try:
                        conf_threat = float(line.split(':', 1)[1].strip())
                    except:
                        pass
                elif line.startswith('CONFIDENCE_TARGET:'):
                    try:
                        conf_target = float(line.split(':', 1)[1].strip())
                    except:
                        pass
            
            # Validate choices
            if domain not in self.DOMAINS:
                domain = 'other'
            if threat not in self.THREAT_LEVELS:
                threat = 'none'
            if target not in self.TARGETS:
                target = 'society'
            
            return {
                'domain': domain,
                'threat_level': threat,
                'target': target,
                'domain_confidence': round(conf_domain, 3),
                'threat_confidence': round(conf_threat, 3),
                'target_confidence': round(conf_target, 3)
            }
            
        except Exception as e:
            print(f"  Error classifying: {str(e)[:50]}")
            return {
                'domain': 'other',
                'threat_level': 'none',
                'target': 'society',
                'domain_confidence': 0.0,
                'threat_confidence': 0.0,
                'target_confidence': 0.0
            }
    
    def classify_batch(self, start_date: str, end_date: str,
                      limit: int = None, batch_size: int = 100):
        """
        Classify all stories in a date range
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Maximum stories to classify
            batch_size: Commit every N stories
        """
        print(f"\n{'='*80}")
        print(f"TOPIC CLASSIFICATION: {start_date} to {end_date}")
        print(f"{'='*80}")
        if limit:
            print(f"Limit: {limit} stories")
        
        # Get unclassified stories
        query = """
            SELECT s.id, s.title
            FROM stories s
            LEFT JOIN story_topics st ON s.id = st.story_id
            WHERE s.created_at >= ? AND s.created_at < ?
            AND s.content_type = 'header'
            AND st.story_id IS NULL
            ORDER BY s.created_at
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        self.db.cursor.execute(query, (start_date, end_date))
        stories = self.db.cursor.fetchall()
        
        total = len(stories)
        print(f"Stories to classify: {total}")
        
        if total == 0:
            print("No new stories to classify!")
            return
        
        # Classify each story
        processed = 0
        for i, (story_id, title) in enumerate(stories, 1):
            # Classify
            result = self.classify(title)
            
            # Store
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.db.cursor.execute("""
                INSERT INTO story_topics
                    (story_id, domain, threat_level, target,
                     domain_confidence, threat_confidence, target_confidence,
                     classified_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                story_id,
                result['domain'],
                result['threat_level'],
                result['target'],
                result['domain_confidence'],
                result['threat_confidence'],
                result['target_confidence'],
                timestamp
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
    
    def analyze_period(self, start_date: str, end_date: str,
                      output_dir: str = "data/analysis"):
        """
        Analyze classifications for a period
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            output_dir: Where to save CSVs
        """
        print(f"\n{'='*80}")
        print(f"TOPIC ANALYSIS: {start_date} to {end_date}")
        print(f"{'='*80}")
        
        # Get all classifications
        query = """
            SELECT s.id, s.title, s.created_at,
                   st.domain, st.threat_level, st.target,
                   st.domain_confidence, st.threat_confidence, st.target_confidence
            FROM story_topics st
            JOIN stories s ON st.story_id = s.id
            WHERE s.created_at >= ? AND s.created_at < ?
            ORDER BY s.created_at
        """
        
        self.db.cursor.execute(query, (start_date, end_date))
        classifications = self.db.cursor.fetchall()
        
        total = len(classifications)
        print(f"Total classified stories: {total}")
        
        if total == 0:
            print("No classifications found!")
            return
        
        # Count distributions
        domains = Counter()
        threats = Counter()
        targets = Counter()
        intersections = Counter()
        
        for row in classifications:
            domain = row[3]
            threat = row[4]
            target = row[5]
            
            domains[domain] += 1
            threats[threat] += 1
            targets[target] += 1
            intersections[(domain, threat, target)] += 1
        
        # Print distributions
        print(f"\n{'='*80}")
        print("DIMENSION DISTRIBUTIONS")
        print(f"{'='*80}")
        
        print(f"\nDOMAINS:")
        for domain, count in domains.most_common():
            pct = (count / total) * 100
            print(f"  {domain:20s}: {count:4d} ({pct:5.1f}%)")
        
        print(f"\nTHREAT LEVELS:")
        for threat, count in threats.most_common():
            pct = (count / total) * 100
            print(f"  {threat:20s}: {count:4d} ({pct:5.1f}%)")
        
        print(f"\nTARGETS:")
        for target, count in targets.most_common():
            pct = (count / total) * 100
            print(f"  {target:20s}: {count:4d} ({pct:5.1f}%)")
        
        print(f"\nTOP 10 INTERSECTIONS (Domain × Threat × Target):")
        for (domain, threat, target), count in intersections.most_common(10):
            print(f"  {domain} + {threat} + {target}: {count}")
        
        # Save CSVs
        os.makedirs(output_dir, exist_ok=True)
        start_str = start_date.replace('-', '')
        end_str = end_date.replace('-', '')
        
        # All classifications
        output_file = os.path.join(output_dir, f"{start_str}_to_{end_str}_topics.csv")
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'story_id', 'title', 'date', 'domain', 'threat_level', 'target',
                'domain_conf', 'threat_conf', 'target_conf'
            ])
            for row in classifications:
                writer.writerow(row)
        
        print(f"\n✓ Saved classifications: {output_file}")
        print(f"{'='*80}")
    
    def close(self):
        """Close database connection"""
        self.db.close()


# Main execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python3 -m src.analyzers.topic_classifier COMMAND START_DATE END_DATE [LIMIT]")
        print("\nCommands:")
        print("  classify - Classify stories")
        print("  analyze  - Analyze classifications")
        print("\nExamples:")
        print("  python3 -m src.analyzers.topic_classifier classify 2024-04-01 2024-07-01 100")
        print("  python3 -m src.analyzers.topic_classifier analyze 2024-04-01 2024-07-01")
        sys.exit(1)
    
    command = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    limit = int(sys.argv[4]) if len(sys.argv) > 4 else None
    
    classifier = TopicClassifier()
    
    if command == 'classify':
        print("="*80)
        print("3-DIMENSIONAL TOPIC CLASSIFICATION")
        print("="*80)
        classifier.classify_batch(start_date, end_date, limit=limit)
    
    elif command == 'analyze':
        print("="*80)
        print("TOPIC CLASSIFICATION ANALYSIS")
        print("="*80)
        classifier.analyze_period(start_date, end_date)
    
    else:
        print(f"Unknown command: {command}")
        print("Use 'classify' or 'analyze'")
    
    classifier.close()
    
    print("\n✅ Topic classification complete!")