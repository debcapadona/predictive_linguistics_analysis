"""
database.py - SQLite database setup and management

Creates and manages the linguistic predictor database
"""

import sqlite3
import os
from typing import List, Dict, Tuple
from datetime import datetime


class Database:
    """Manage SQLite database for linguistic predictor"""
    
    def __init__(self, db_path: str = "data/linguistic_predictor.db"):
        """
        Initialize database connection
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        
        # Create directory if needed
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        self.cursor = self.conn.cursor()
        
        print(f"✓ Connected to database: {db_path}")
    
    def create_tables(self):
        """Create database tables"""
        print("\nCreating database tables...")
        
        # Sources table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                type TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Stories table (handles both headers and comments)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS stories (
                id TEXT PRIMARY KEY,
                source_id INTEGER NOT NULL,
                content_type TEXT NOT NULL,  -- 'header' or 'comment'
                title TEXT NOT NULL,
                url TEXT,
                created_at TIMESTAMP NOT NULL,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                parent_story_id TEXT,  -- For comments, links to parent story
                author TEXT,  -- For comments, who wrote it
                comment_depth INTEGER,  -- For comments, how deep in thread
                FOREIGN KEY (source_id) REFERENCES sources(id)
            )
        """)
        
        # Processed text table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_text (
                story_id TEXT PRIMARY KEY,
                words TEXT,
                bigrams TEXT,
                trigrams TEXT,
                word_count INTEGER,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (story_id) REFERENCES stories(id)
            )
        """)
        
        # Create indexes for fast queries
        # Add new index for content_type filtering
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_stories_content_type 
            ON stories(content_type)
        """)

        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_stories_parent 
            ON stories(parent_story_id)
        """)

        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_stories_date 
            ON stories(created_at)
        """)
        
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_stories_source 
            ON stories(source_id)
        """)
        
        self.conn.commit()
        print("✓ Tables created successfully")

# Word co-occurrences table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS word_cooccurrences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                word1 TEXT NOT NULL,
                word2 TEXT NOT NULL,
                cooccurrence_count INTEGER,
                time_window TEXT,
                date_start DATE,
                date_end DATE,
                source_id INTEGER,
                content_type TEXT,
                FOREIGN KEY (source_id) REFERENCES sources(id)
            )
        """)
        
        # Temporal markers table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS temporal_markers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                story_id TEXT NOT NULL,
                marker_text TEXT NOT NULL,
                marker_type TEXT,
                context_words TEXT,
                predicted_timeframe TEXT,
                created_at TIMESTAMP,
                FOREIGN KEY (story_id) REFERENCES stories(id)
            )
        """)
        
        # Tension scores table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS tension_scores (
                story_id TEXT PRIMARY KEY,
                tension_score REAL,
                release_score REAL,
                net_tension REAL,
                uncertainty_count INTEGER,
                urgency_count INTEGER,
                conflict_count INTEGER,
                calculated_at TIMESTAMP,
                FOREIGN KEY (story_id) REFERENCES stories(id)
            )
        """)
        
        # Indexes for new tables
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_cooccur_words 
            ON word_cooccurrences(word1, word2)
        """)
        
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_cooccur_date 
            ON word_cooccurrences(date_start, date_end)
        """)
        
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_temporal_story 
            ON temporal_markers(story_id)
        """)
        
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_temporal_date 
            ON temporal_markers(created_at)
        """)

# Topic classifications table (iterative refinement)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS topic_classifications (
                story_id TEXT PRIMARY KEY,
                iteration_1 TEXT,
                iteration_2 TEXT,
                iteration_3 TEXT,
                iteration_4 TEXT,
                confidence_1 REAL,
                confidence_2 REAL,
                confidence_3 REAL,
                confidence_4 REAL,
                classified_at TIMESTAMP,
                FOREIGN KEY (story_id) REFERENCES stories(id)
            )
        """)
        
        # Index for topic queries
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_topic_story 
            ON topic_classifications(story_id)
        """)
        
    def add_source(self, name: str, source_type: str) -> int:
        """
        Add a data source
        
        Args:
            name: Source name (e.g., "Hacker News")
            source_type: Source type (e.g., "hackernews", "rss")
            
        Returns:
            Source ID
        """
        try:
            self.cursor.execute(
                "INSERT INTO sources (name, type) VALUES (?, ?)",
                (name, source_type)
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.IntegrityError:
            # Source already exists, get its ID
            self.cursor.execute(
                "SELECT id FROM sources WHERE name = ?",
                (name,)
            )
            return self.cursor.fetchone()[0]
    
    def add_story(self, story: Dict, source_id: int):
        """
        Add a story to database
        
        Args:
            story: Story dictionary
            source_id: ID of source
        """
        try:
            self.cursor.execute("""
                INSERT INTO stories (id, source_id, content_type, title, url, created_at,
                                parent_story_id, author, comment_depth)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                story['id'],
                source_id,
                story.get('content_type', 'header'),
                story['title'],
                story.get('url', ''),
                story['created_at'],
                story.get('parent_story_id'),
                story.get('author'),
                story.get('comment_depth')
            ))
        except sqlite3.IntegrityError:
            # Story already exists, skip
            pass
    
    def add_processed_text(self, story_id: str, processed: Dict):
        """
        Add processed text for a story
        
        Args:
            story_id: Story ID
            processed: Processed text dictionary
        """
        try:
            self.cursor.execute("""
                INSERT INTO processed_text (story_id, words, bigrams, trigrams, word_count)
                VALUES (?, ?, ?, ?, ?)
            """, (
                story_id,
                processed['words'],
                processed['bigrams'],
                processed['trigrams'],
                processed['word_count']
            ))
        except sqlite3.IntegrityError:
            # Already processed, skip
            pass
    
    def get_stories_by_date_range(self, start_date: str, end_date: str, 
                                  source_id: int = None) -> List[Dict]:
        """
        Get stories within date range
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            source_id: Optional source filter
            
        Returns:
            List of story dictionaries
        """
        if source_id:
            query = """
                SELECT s.*, p.words, p.bigrams, p.trigrams, p.word_count
                FROM stories s
                LEFT JOIN processed_text p ON s.id = p.story_id
                WHERE s.created_at >= ? AND s.created_at < ? AND s.source_id = ?
                ORDER BY s.created_at
            """
            self.cursor.execute(query, (start_date, end_date, source_id))
        else:
            query = """
                SELECT s.*, p.words, p.bigrams, p.trigrams, p.word_count
                FROM stories s
                LEFT JOIN processed_text p ON s.id = p.story_id
                WHERE s.created_at >= ? AND s.created_at < ?
                ORDER BY s.created_at
            """
            self.cursor.execute(query, (start_date, end_date))
        
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_stats(self) -> Dict:
        """Get database statistics"""
        stats = {}
        
        # Total stories
        self.cursor.execute("SELECT COUNT(*) FROM stories")
        stats['total_stories'] = self.cursor.fetchone()[0]
        
        # Total processed
        self.cursor.execute("SELECT COUNT(*) FROM processed_text")
        stats['total_processed'] = self.cursor.fetchone()[0]
        
        # Stories per source
        self.cursor.execute("""
            SELECT s.name, s.type, COUNT(st.id) as count
            FROM sources s
            LEFT JOIN stories st ON s.id = st.source_id
            GROUP BY s.id
        """)
        stats['by_source'] = [dict(row) for row in self.cursor.fetchall()]
        
        # Date range
        self.cursor.execute("""
            SELECT MIN(created_at) as earliest, MAX(created_at) as latest
            FROM stories
        """)
        date_range = self.cursor.fetchone()
        stats['date_range'] = {
            'earliest': date_range[0],
            'latest': date_range[1]
        }
        
        return stats
    
    def close(self):
        """Close database connection"""
        self.conn.commit()
        self.conn.close()
        print("✓ Database connection closed")


# Main execution for testing
if __name__ == "__main__":
    print("=" * 80)
    print("DATABASE SETUP")
    print("=" * 80)
    
    # Create database
    db = Database()
    
    # Create tables
    db.create_tables()
    
    # Add sample sources
    hn_id = db.add_source("Hacker News", "hackernews")
    rss_id = db.add_source("RSS Feeds", "rss")
    
    print(f"\n✓ Added sources: HN (id={hn_id}), RSS (id={rss_id})")
    
    # Show stats
    print("\nDatabase Statistics:")
    stats = db.get_stats()
    print(f"  Total stories: {stats['total_stories']}")
    print(f"  Total processed: {stats['total_processed']}")
    print(f"  Sources: {len(stats['by_source'])}")
    
    db.close()
    
    print("\n" + "=" * 80)
    print("Database ready at: data/linguistic_predictor.db")
    print("=" * 80)