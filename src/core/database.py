"""
Database adapter supporting both SQLite and PostgreSQL
"""

import sqlite3
from typing import List, Tuple, Any, Optional, Dict
from pathlib import Path


class Database:
    """Unified database interface supporting SQLite and PostgreSQL"""
    
    def __init__(self, db_type: str = 'sqlite', **kwargs):
        """
        Initialize database connection
        
        Args:
            db_type: 'sqlite' or 'postgresql'
            **kwargs: Database-specific connection parameters
                SQLite: db_path
                PostgreSQL: host, port, database, user, password
        """
        self.db_type = db_type.lower()
        self.conn = None
        self.cursor = None
        
        if self.db_type == 'sqlite':
            self._init_sqlite(kwargs.get('db_path', 'data/linguistic_predictor.db'))
        elif self.db_type == 'postgresql':
            self._init_postgresql(
                host=kwargs.get('host', 'localhost'),
                port=kwargs.get('port', 5432),
                database=kwargs.get('database'),
                user=kwargs.get('user'),
                password=kwargs.get('password')
            )
        else:
            raise ValueError(f"Unsupported db_type: {db_type}. Use 'sqlite' or 'postgresql'")
    
    def _init_sqlite(self, db_path: str):
        """Initialize SQLite connection"""
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        print(f"✓ Connected to SQLite: {db_path}")
    
    def _init_postgresql(self, host: str, port: int, database: str, user: str, password: str):
        """Initialize PostgreSQL connection"""
        try:
            import psycopg2
            self.conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            self.cursor = self.conn.cursor()
            print(f"✓ Connected to PostgreSQL: {database}@{host}:{port}")
        except ImportError:
            raise ImportError("psycopg2 not installed. Run: pip install psycopg2-binary")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")
    
    def execute_query(self, query: str, params: Tuple = None) -> List[Tuple]:
        """
        Execute a SELECT query and return results
        
        Args:
            query: SQL query
            params: Query parameters
            
        Returns:
            List of result tuples
        """
        if params is None:
            params = ()
        
        # Convert ? to %s for PostgreSQL
        if self.db_type == 'postgresql':
            query = query.replace('?', '%s')
        
        self.cursor.execute(query, params)
        return self.cursor.fetchall()
    
    def execute_update(self, query: str, params: Tuple = None) -> int:
        """
        Execute an INSERT/UPDATE/DELETE query
        
        Args:
            query: SQL query
            params: Query parameters
            
        Returns:
            Number of affected rows
        """
        if params is None:
            params = ()
        
        # Convert ? to %s for PostgreSQL
        if self.db_type == 'postgresql':
            query = query.replace('?', '%s')
        
        self.cursor.execute(query, params)
        self.conn.commit()
        return self.cursor.rowcount
    
    def get_last_insert_id(self) -> int:
        """Get the last inserted row ID"""
        if self.db_type == 'sqlite':
            return self.cursor.lastrowid
        else:  # PostgreSQL
            self.cursor.execute("SELECT lastval()")
            return self.cursor.fetchone()[0]
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    # ========== SQLite-Specific Methods ==========
    
    def create_tables(self):
        """Create SQLite database tables"""
        if self.db_type != 'sqlite':
            return
            
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
        
        # Stories table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS stories (
                id TEXT PRIMARY KEY,
                source_id INTEGER NOT NULL,
                content_type TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT,
                created_at TIMESTAMP NOT NULL,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                parent_story_id TEXT,
                author TEXT,
                comment_depth INTEGER,
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
        
        # Indexes
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_stories_content_type ON stories(content_type)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_stories_parent ON stories(parent_story_id)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_stories_date ON stories(created_at)")
        
        self.conn.commit()
        print("✓ Tables created")
    
    def add_source(self, name: str, source_type: str) -> int:
        """Add a data source, return source ID"""
        try:
            self.cursor.execute("INSERT INTO sources (name, type) VALUES (?, ?)", (name, source_type))
            self.conn.commit()
            return self.cursor.lastrowid
        except:
            self.cursor.execute("SELECT id FROM sources WHERE name = ?", (name,))
            return self.cursor.fetchone()[0]
    
    def add_story(self, story: Dict, source_id: int):
        """Add a story to database"""
        self.cursor.execute("""
            INSERT INTO stories (id, source_id, content_type, title, url, created_at,
                                parent_story_id, author, comment_depth)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            story['id'], source_id, story.get('content_type', 'header'),
            story['title'], story.get('url', ''), story['created_at'],
            story.get('parent_story_id'), story.get('author'), story.get('comment_depth')
        ))
        self.conn.commit()
    
    def add_processed_text(self, story_id: str, processed: Dict):
        """Add processed text for a story"""
        self.cursor.execute("""
            INSERT OR REPLACE INTO processed_text (story_id, words, bigrams, trigrams, word_count)
            VALUES (?, ?, ?, ?, ?)
        """, (story_id, processed['words'], processed['bigrams'], processed['trigrams'], processed['word_count']))
        self.conn.commit()
    
    # ========== MDC-Specific Methods ==========
    
    def get_or_create_classification(
        self,
        certainty_score: float,
        pronoun_first: float,
        pronoun_collective: float,
        valence_score: float,
        temporal_bleed: float,
        time_compression: float,
        sacred_profane: float,
        temporal_proximity: float = 0.5,
        novel_meme: float = 0.0,
        agency_reversal: float = 0.0,
        metaphor_density: float = 0.0
    ) -> int:
        """
        Get existing classification ID or create new one
        Deduplicates identical vectors
        
        Returns:
            classification_id
        """
        # Round to 3 decimals for comparison
        certainty_score = round(certainty_score, 3)
        pronoun_first = round(pronoun_first, 3)
        pronoun_collective = round(pronoun_collective, 3)
        valence_score = round(valence_score, 3)
        temporal_bleed = round(temporal_bleed, 3)
        time_compression = round(time_compression, 3)
        sacred_profane = round(sacred_profane, 3)
        temporal_proximity = round(temporal_proximity, 3)
        
        params = (
            certainty_score,
            pronoun_first,
            pronoun_collective,
            valence_score,
            temporal_bleed,
            time_compression,
            sacred_profane,
            temporal_proximity,
            novel_meme,
            agency_reversal,
            metaphor_density
        )
        
        if self.db_type == 'postgresql':
            # PostgreSQL: SELECT first, insert only if not exists
            select_query = """
                SELECT id FROM mdc_classifications
                WHERE certainty_score = %s
                  AND pronoun_first = %s
                  AND pronoun_collective = %s
                  AND valence_score = %s
                  AND temporal_bleed = %s
                  AND time_compression = %s
                  AND sacred_profane = %s
                  AND temporal_proximity = %s
            """
            
            select_params = params[:8]
            existing = self.execute_query(select_query, select_params)
            
            if existing:
                return existing[0][0]
            
            # Doesn't exist, insert it
            insert_query = """
                INSERT INTO mdc_classifications (
                    certainty_score, pronoun_first, pronoun_collective,
                    valence_score, temporal_bleed, time_compression,
                    sacred_profane, temporal_proximity,
                    novel_meme, agency_reversal, metaphor_density
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            
            try:
                self.cursor.execute(insert_query, params)
                self.conn.commit()
                return self.cursor.fetchone()[0]
            except:
                # Race condition: someone else inserted between SELECT and INSERT
                self.conn.rollback()
                existing = self.execute_query(select_query, select_params)
                if existing:
                    return existing[0][0]
                else:
                    raise
            
        else:
            # SQLite: check first, then insert if needed
            query = """
                SELECT id FROM mdc_classifications
                WHERE certainty_score = ?
                  AND pronoun_first = ?
                  AND pronoun_collective = ?
                  AND valence_score = ?
                  AND temporal_bleed = ?
                  AND time_compression = ?
                  AND sacred_profane = ?
                  AND temporal_proximity = ?
            """
            
            existing = self.execute_query(query, params[:8])
            
            if existing:
                return existing[0][0]
            
            # Create new
            insert_query = """
                INSERT INTO mdc_classifications (
                    certainty_score, pronoun_first, pronoun_collective,
                    valence_score, temporal_bleed, time_compression,
                    sacred_profane, temporal_proximity,
                    novel_meme, agency_reversal, metaphor_density
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            self.execute_update(insert_query, params)
            return self.get_last_insert_id()
    
    def add_story_classification(self, story_id: int, classification_id: int) -> bool:
        """
        Link a story to a classification
        
        Args:
            story_id: Story ID
            classification_id: Classification ID
            
        Returns:
            True if successful
        """
        query = """
            INSERT INTO story_classifications (story_id, classification_id)
            VALUES (%s, %s)
            ON CONFLICT (story_id) DO NOTHING
        """
        
        try:
            self.execute_update(query, (story_id, classification_id))
            return True
        except Exception as e:
            print(f"Error adding story classification: {e}")
            if self.db_type == 'postgresql':
                self.conn.rollback()
            return False
    
    def add_word_tokens(
        self,
        story_id: int,
        classification_id: int,
        words: List[str]  # Just words, no position
    ) -> int:
        """
        Add word tokens for a story
        
        Args:
            story_id: Story ID
            classification_id: Classification ID
            words: List of words (strings)
            
        Returns:
            Number of words inserted
        """
        if not words:
            return 0
        
        query = """
            INSERT INTO word_tokens (
                story_id, word_text, word_lower, position, classification_id
            ) VALUES (%s, %s, %s, %s, %s)
        """
        
        rows = [
            (story_id, word, word.lower(), idx, classification_id)
            for idx, word in enumerate(words)
        ]
        
        if self.db_type == 'postgresql':
            # PostgreSQL: use executemany
            self.cursor.executemany(query, rows)
            self.conn.commit()
            return len(rows)
        else:
            # SQLite: use executemany with ? placeholders
            query = query.replace('%s', '?')
            self.cursor.executemany(query, rows)
            self.conn.commit()
            return len(rows)
    
    def get_story_classification(self, story_id: int) -> Optional[dict]:
        """
        Get the classification for a story
        
        Args:
            story_id: Story ID
            
        Returns:
            Dict with classification data or None
        """
        query = """
            SELECT 
                mc.certainty_score,
                mc.pronoun_first,
                mc.pronoun_collective,
                mc.valence_score,
                mc.temporal_bleed,
                mc.time_compression,
                mc.sacred_profane,
                mc.temporal_proximity
            FROM mdc_classifications mc
            JOIN story_classifications sc ON sc.classification_id = mc.id
            WHERE sc.story_id = %s
        """
        
        result = self.execute_query(query, (story_id,))
        
        if not result:
            return None
        
        row = result[0]
        return {
            'certainty_score': row[0],
            'pronoun_first': row[1],
            'pronoun_collective': row[2],
            'valence_score': row[3],
            'temporal_bleed': row[4],
            'time_compression': row[5],
            'sacred_profane': row[6],
            'temporal_proximity': row[7]
        }
    
    def get_words_by_dimension(
        self,
        dimension: str,
        min_score: float = None,
        max_score: float = None,
        limit: int = 100
    ) -> List[Tuple[str, int, float]]:
        """
        Get words filtered by dimensional score
        
        Args:
            dimension: Dimension name (certainty_score, valence_score, etc.)
            min_score: Minimum score (optional)
            max_score: Maximum score (optional)
            limit: Result limit
            
        Returns:
            List of (word, count, avg_score) tuples
        """
        valid_dimensions = {
            'certainty_score', 'pronoun_first', 'pronoun_collective',
            'valence_score', 'temporal_bleed', 'time_compression',
            'sacred_profane', 'temporal_proximity'
        }
        
        if dimension not in valid_dimensions:
            raise ValueError(f"Invalid dimension: {dimension}")
        
        where_clauses = []
        params = []
        
        if min_score is not None:
            where_clauses.append(f"mc.{dimension} >= %s")
            params.append(min_score)
        
        if max_score is not None:
            where_clauses.append(f"mc.{dimension} <= %s")
            params.append(max_score)
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        query = f"""
            SELECT 
                wt.word_lower,
                COUNT(*) as frequency,
                AVG(mc.{dimension}) as avg_score
            FROM word_tokens wt
            JOIN mdc_classifications mc ON wt.classification_id = mc.id
            WHERE {where_sql}
            GROUP BY wt.word_lower
            ORDER BY frequency DESC
            LIMIT %s
        """
        
        params.append(limit)
        
        return self.execute_query(query, tuple(params))


# Test connection
if __name__ == "__main__":
    # Test SQLite
    print("Testing SQLite connection...")
    sqlite_db = Database(db_type='sqlite', db_path='data/linguistic_predictor.db')
    sqlite_db.create_tables()
    
    # Test adding source
    source_id = sqlite_db.add_source("Test Source", "test")
    print(f"  Added source: {source_id}")
    
    sqlite_db.close()
    print("✓ SQLite tests passed")