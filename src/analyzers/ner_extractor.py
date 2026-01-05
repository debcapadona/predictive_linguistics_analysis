"""
ner_extractor.py - Named Entity Recognition Extractor

Extracts people, organizations, locations, and other entities from stories
Uses spaCy for fast, accurate NER
"""

import sys
import os
import csv
from datetime import datetime
from collections import Counter
from typing import List, Dict, Tuple
import time
import sqlite3

def execute_with_retry(cursor, query, params=None, max_retries=10, wait_seconds=5):
    """Execute SQL with retry on database lock"""
    for attempt in range(max_retries):
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return True
        except sqlite3.OperationalError as e:
            if 'locked' in str(e).lower() and attempt < max_retries - 1:
                print(f"  Database locked, retry {attempt + 1}/{max_retries}...", end='\r')
                time.sleep(wait_seconds)
                continue
            else:
                raise
    return False


# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.database import Database


class NERExtractor:
    """Extract named entities using spaCy"""
    
    def __init__(self, db_path: str = "data/linguistic_predictor.db"):
        """Initialize NER extractor"""
        self.db = Database(db_path)
        
        # Initialize spaCy
        try:
            import spacy
            print("Loading spaCy model...")
            self.nlp = spacy.load("en_core_web_sm")
            self.available = True
            print("✓ spaCy NER available")
        except ImportError:
            print("⚠️  spaCy not installed. Install with: pip install spacy")
            print("   Then download model: python -m spacy download en_core_web_sm")
            self.nlp = None
            self.available = False
        except OSError:
            print("⚠️  spaCy model not found. Download with:")
            print("   python -m spacy download en_core_web_sm")
            self.nlp = None
            self.available = False
        
        # Create entities table if needed
        self._create_table()
    
    def _create_table(self):
        """Create named_entities table if it doesn't exist"""
        self.db.cursor.execute("""
            CREATE TABLE IF NOT EXISTS named_entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                story_id TEXT NOT NULL,
                entity_text TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                start_char INTEGER,
                end_char INTEGER,
                extracted_at TIMESTAMP,
                FOREIGN KEY (story_id) REFERENCES stories(id)
            )
        """)
        
        # Create index for fast queries
        self.db.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_entities_story 
            ON named_entities(story_id)
        """)
        
        self.db.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_entities_type 
            ON named_entities(entity_type)
        """)
        
        self.db.conn.commit()
        print("✓ named_entities table ready")
    
    def extract_entities(self, text: str) -> List[Dict]:
        """
        Extract named entities from text
        
        Args:
            text: Text to analyze
            
        Returns:
            List of entity dictionaries
        """
        if not self.available or not text:
            return []
        
        doc = self.nlp(text)
        
        entities = []
        for ent in doc.ents:
            entities.append({
                'text': ent.text,
                'type': ent.label_,
                'start': ent.start_char,
                'end': ent.end_char
            })
        
        return entities
    
    def process_stories(self, start_date: str, end_date: str, 
                       limit: int = None, content_type: str = 'header'):
        """
        Extract entities from all stories in period
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Maximum stories to process
            content_type: 'header' or 'comment'
        """
        print(f"\n{'='*80}")
        print(f"NER EXTRACTION: {start_date} to {end_date}")
        print(f"{'='*80}")
        print(f"Content type: {content_type}")
        if limit:
            print(f"Limit: {limit} stories")
        
        # Get stories that haven't been processed yet
        query = """
            SELECT s.id, s.title, s.created_at
            FROM stories s
            LEFT JOIN named_entities ne ON s.id = ne.story_id
            WHERE s.created_at >= ? AND s.created_at < ?
            AND s.content_type = ?
            AND ne.story_id IS NULL
            GROUP BY s.id
            ORDER BY s.created_at
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        self.db.cursor.execute(query, (start_date, end_date, content_type))
        stories = self.db.cursor.fetchall()
        
        total = len(stories)
        print(f"Stories to process: {total}")
        
        if total == 0:
            print("No new stories to process!")
            return
        
        # Process each story
        entities_added = 0
        for i, (story_id, title, created_at) in enumerate(stories, 1):
            # Extract entities
            entities = self.extract_entities(title)
            
            # Store in database
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for ent in entities:
                execute_with_retry(
                    self.db.cursor,
                    """
                    INSERT INTO named_entities 
                        (story_id, entity_text, entity_type, start_char, end_char, extracted_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        story_id,
                        ent['text'],
                        ent['type'],
                        ent['start'],
                        ent['end'],
                        timestamp
                    )
                )
                entities_added += 1
            
            # Commit in batches
            if i % 100 == 0:
                for retry in range(10):
                    try:
                        self.db.conn.commit()
                        break
                    except sqlite3.OperationalError as e:
                        if 'locked' in str(e).lower() and retry < 9:
                            time.sleep(5)
                            continue
                        raise
        
        # Final commit
        self.db.conn.commit()
        
        print(f"\n✓ Extracted {entities_added} entities from {total} stories")
    
    def analyze_period(self, start_date: str, end_date: str, 
                      output_dir: str = "data/analysis") -> Dict:
        """
        Analyze extracted entities for a period
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            output_dir: Where to save analysis CSVs
            
        Returns:
            Dictionary with analysis results
        """
        print(f"\n{'='*80}")
        print(f"NER ANALYSIS: {start_date} to {end_date}")
        print(f"{'='*80}")
        
        # Get all entities for period
        query = """
            SELECT ne.entity_text, ne.entity_type, s.created_at
            FROM named_entities ne
            JOIN stories s ON ne.story_id = s.id
            WHERE s.created_at >= ? AND s.created_at < ?
            ORDER BY s.created_at
        """
        
        self.db.cursor.execute(query, (start_date, end_date))
        entities = self.db.cursor.fetchall()
        
        total = len(entities)
        print(f"Total entities found: {total}")
        
        if total == 0:
            print("No entities found for this period!")
            return {}
        
        # Count by type
        by_type = Counter()
        by_entity = Counter()
        entities_by_type = {}
        
        for text, ent_type, created_at in entities:
            by_type[ent_type] += 1
            by_entity[(text, ent_type)] += 1
            
            if ent_type not in entities_by_type:
                entities_by_type[ent_type] = Counter()
            entities_by_type[ent_type][text] += 1
        
        # Print summary
        print(f"\nEntity types distribution:")
        for ent_type, count in by_type.most_common():
            pct = (count / total) * 100
            print(f"  {ent_type:20s}: {count:5d} ({pct:5.1f}%)")
        
        # Save CSVs
        os.makedirs(output_dir, exist_ok=True)
        start_str = start_date.replace('-', '')
        end_str = end_date.replace('-', '')
        
        # All entities
        all_file = os.path.join(output_dir, f"{start_str}_to_{end_str}_entities_all.csv")
        with open(all_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['entity', 'type', 'count', 'rank'])
            for rank, ((text, ent_type), count) in enumerate(by_entity.most_common(), 1):
                writer.writerow([text, ent_type, count, rank])
        print(f"\n✓ Saved all entities: {all_file}")
        
        # Top entities by type
        for ent_type, entities in entities_by_type.items():
            type_file = os.path.join(output_dir, f"{start_str}_to_{end_str}_entities_{ent_type.lower()}.csv")
            with open(type_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['entity', 'count', 'rank'])
                for rank, (text, count) in enumerate(entities.most_common(50), 1):
                    writer.writerow([text, count, rank])
            print(f"✓ Saved {ent_type}: {type_file}")
        
        # Show top entities by type
        print(f"\n{'='*80}")
        print("TOP ENTITIES BY TYPE")
        print(f"{'='*80}")
        
        for ent_type in ['PERSON', 'ORG', 'GPE', 'DATE', 'PRODUCT']:
            if ent_type in entities_by_type:
                print(f"\nTop {ent_type}:")
                for text, count in entities_by_type[ent_type].most_common(10):
                    print(f"  {text:40s}: {count:3d}")
        
        print(f"{'='*80}")
        
        return {
            'total': total,
            'by_type': dict(by_type),
            'by_entity': dict(by_entity)
        }
    
    def close(self):
        """Close database connection"""
        self.db.close()


# Main execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python3 -m src.analyzers.ner_extractor COMMAND START_DATE END_DATE [LIMIT]")
        print("\nCommands:")
        print("  extract  - Extract entities from stories")
        print("  analyze  - Analyze extracted entities")
        print("\nExamples:")
        print("  python3 -m src.analyzers.ner_extractor extract 2024-04-01 2024-07-01 1000")
        print("  python3 -m src.analyzers.ner_extractor analyze 2024-04-01 2024-07-01")
        sys.exit(1)
    
    command = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    limit = int(sys.argv[4]) if len(sys.argv) > 4 else None
    
    extractor = NERExtractor()
    
    if command == 'extract':
        print("="*80)
        print("NAMED ENTITY EXTRACTION")
        print("="*80)
        extractor.process_stories(start_date, end_date, limit=limit)
    
    elif command == 'analyze':
        print("="*80)
        print("NAMED ENTITY ANALYSIS")
        print("="*80)
        extractor.analyze_period(start_date, end_date)
    
    else:
        print(f"Unknown command: {command}")
        print("Use 'extract' or 'analyze'")
    
    extractor.close()
    
    print("\n✅ NER processing complete!")