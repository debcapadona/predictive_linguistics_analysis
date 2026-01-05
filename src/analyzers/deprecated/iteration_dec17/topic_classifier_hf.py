"""
topic_classifier_hf.py - Multi-Dimensional Topic Classification (HuggingFace)

FREE classification using HuggingFace zero-shot model
"""

import sys
import os
import csv
from datetime import datetime
from typing import Dict, List
from collections import Counter

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.database import Database


class TopicClassifierHF:
    """3-Dimensional topic classification using HuggingFace (FREE)"""
    
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
        'none', 'concern', 'crisis', 'catastrophe'
    ]
    
    TARGETS = [
        'individual', 'company', 'industry', 'government',
        'society', 'infrastructure', 'nature'
    ]
    
    def __init__(self, db_path: str = "data/linguistic_predictor.db"):
        """Initialize HuggingFace classifier"""
        self.db = Database(db_path)
        self.available = False
        
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
            print("⚠️  transformers not installed")
            print("   Install with: pip install transformers torch")
        
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
        """Classify story across 3 dimensions using HuggingFace"""
        if not self.available or not title:
            return {
                'domain': 'other',
                'threat_level': 'none',
                'target': 'society',
                'domain_confidence': 0.0,
                'threat_confidence': 0.0,
                'target_confidence': 0.0
            }
        
        try:
            # Truncate title
            text = title[:512]
            
            # Classify domain
            domain_result = self.classifier(
                text,
                candidate_labels=self.DOMAINS,
                multi_label=False
            )
            domain = domain_result['labels'][0]
            domain_conf = domain_result['scores'][0]
            
            # Classify threat level
            threat_result = self.classifier(
                text,
                candidate_labels=self.THREAT_LEVELS,
                multi_label=False
            )
            threat = threat_result['labels'][0]
            threat_conf = threat_result['scores'][0]
            
            # Classify target
            target_result = self.classifier(
                text,
                candidate_labels=self.TARGETS,
                multi_label=False
            )
            target = target_result['labels'][0]
            target_conf = target_result['scores'][0]
            
            return {
                'domain': domain,
                'threat_level': threat,
                'target': target,
                'domain_confidence': round(domain_conf, 3),
                'threat_confidence': round(threat_conf, 3),
                'target_confidence': round(target_conf, 3)
            }
            
        except Exception as e:
            print(f"  HF error: {str(e)[:30]}")
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
        """Classify all stories in a date range"""
        print(f"\n{'='*80}")
        print(f"TOPIC CLASSIFICATION (HuggingFace): {start_date} to {end_date}")
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
            result = self.classify(title)
            
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
            
            if processed % batch_size == 0:
                self.db.conn.commit()
                progress = (processed / total) * 100
                print(f"  Progress: {progress:.1f}% ({processed}/{total})", end='\r')
        
        self.db.conn.commit()
        print(f"\n✓ Classified {processed} stories")
    
    def analyze_period(self, start_date: str, end_date: str,
                      output_dir: str = "data/analysis"):
        """Analyze classifications for a period"""
        print(f"\n{'='*80}")
        print(f"TOPIC ANALYSIS: {start_date} to {end_date}")
        print(f"{'='*80}")
        
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
        
        os.makedirs(output_dir, exist_ok=True)
        start_str = start_date.replace('-', '')
        end_str = end_date.replace('-', '')
        
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
        print("Usage: python3 -m src.analyzers.topic_classifier_hf COMMAND START_DATE END_DATE [LIMIT]")
        print("\nCommands:")
        print("  classify - Classify stories (FREE)")
        print("  analyze  - Analyze classifications")
        sys.exit(1)
    
    command = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    limit = int(sys.argv[4]) if len(sys.argv) > 4 else None
    
    classifier = TopicClassifierHF()
    
    if command == 'classify':
        print("="*80)
        print("3-DIMENSIONAL TOPIC CLASSIFICATION (HuggingFace - FREE)")
        print("="*80)
        classifier.classify_batch(start_date, end_date, limit=limit)
    
    elif command == 'analyze':
        print("="*80)
        print("TOPIC CLASSIFICATION ANALYSIS")
        print("="*80)
        classifier.analyze_period(start_date, end_date)
    
    else:
        print(f"Unknown command: {command}")
    
    classifier.close()
    
    print("\n✅ Topic classification complete!")