"""
Detect and tag temporal markers in comments
Identifies urgency/temporal language patterns
"""
import psycopg2
import pandas as pd
from datetime import datetime

print("="*70)
print("TEMPORAL MARKER DETECTION")
print("="*70)

# Temporal marker vocabulary
TEMPORAL_MARKERS = {
    'urgency': [
        'urgent', 'immediately', 'asap', 'critical', 'emergency', 'breaking',
        'now', 'right now', 'hurry', 'rush', 'crisis', 'alert'
    ],
    'near_future': [
        'soon', 'shortly', 'upcoming', 'imminent', 'coming', 'approaching',
        'tomorrow', 'next', 'about to', 'going to', 'will be'
    ],
    'sudden_change': [
        'suddenly', 'abruptly', 'unexpected', 'surprise', 'shock', 'dramatic',
        'rapid', 'quick', 'fast', 'swift', 'instant', 'overnight'
    ],
    'temporal_pressure': [
        'deadline', 'time limit', 'running out', 'expire', 'countdown',
        'before', 'until', 'by the time', 'too late', 'last chance'
    ],
    'happening_now': [
        'currently', 'happening', 'ongoing', 'live', 'real-time', 
        'in progress', 'underway', 'unfolding', 'developing'
    ]
}

print("\nTemporal marker categories:")
for category, words in TEMPORAL_MARKERS.items():
    print(f"  {category}: {len(words)} words")

pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)

cur = pg_conn.cursor()

# Create temporal markers table
print("\nCreating temporal_markers table...")
cur.execute("""
    CREATE TABLE IF NOT EXISTS temporal_markers (
        id SERIAL PRIMARY KEY,
        story_id TEXT NOT NULL,
        marker_category VARCHAR(50) NOT NULL,
        marker_word VARCHAR(50) NOT NULL,
        word_count INTEGER NOT NULL,
        created_at TIMESTAMP NOT NULL,
        FOREIGN KEY (story_id) REFERENCES stories(id)
    )
""")

cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_temporal_markers_story 
    ON temporal_markers(story_id)
""")

cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_temporal_markers_date 
    ON temporal_markers(created_at)
""")

cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_temporal_markers_category 
    ON temporal_markers(marker_category)
""")

pg_conn.commit()
print("  ✓ Table created")

# Clear existing data
cur.execute("DELETE FROM temporal_markers")
pg_conn.commit()

# Scan word_tokens for temporal markers
print("\nScanning word_tokens for temporal markers...")

total_markers = 0

for category, words in TEMPORAL_MARKERS.items():
    print(f"\n  Processing category: {category}")
    
    for word in words:
        # Find stories containing this temporal marker
        query = """
            SELECT 
                wt.story_id,
                s.created_at,
                COUNT(*) as word_count
            FROM word_tokens wt
            JOIN stories s ON wt.story_id = s.id
            WHERE LOWER(wt.word_text) = %s
            AND DATE(s.created_at) BETWEEN '2024-01-01' AND '2024-11-30'
            GROUP BY wt.story_id, s.created_at
        """
        
        cur.execute(query, (word.lower(),))
        results = cur.fetchall()
        
        if results:
            # Insert temporal markers
            for story_id, created_at, count in results:
                cur.execute("""
                    INSERT INTO temporal_markers 
                    (story_id, marker_category, marker_word, word_count, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (story_id, category, word, count, created_at))
            
            total_markers += len(results)
            print(f"    {word}: {len(results)} stories")
    
    pg_conn.commit()

print(f"\n✓ Total temporal markers tagged: {total_markers:,}")

# Get daily temporal marker density
print("\nCalculating daily temporal marker density...")

query = """
    SELECT 
        DATE(created_at) as date,
        marker_category,
        COUNT(DISTINCT story_id) as stories_with_markers,
        SUM(word_count) as total_marker_words
    FROM temporal_markers
    GROUP BY DATE(created_at), marker_category
    ORDER BY date, marker_category
"""

df = pd.read_sql(query, pg_conn)
df.to_csv('data/temporal_marker_density.csv', index=False)
print("  ✓ Saved: data/temporal_marker_density.csv")

# Summary stats
print("\nTemporal marker summary by category:")
cur.execute("""
    SELECT 
        marker_category,
        COUNT(DISTINCT story_id) as unique_stories,
        SUM(word_count) as total_occurrences
    FROM temporal_markers
    GROUP BY marker_category
    ORDER BY total_occurrences DESC
""")

for category, stories, occurrences in cur.fetchall():
    print(f"  {category:20s} | {stories:6,} stories | {occurrences:8,} occurrences")

pg_conn.close()

print("\n" + "="*70)
print("TEMPORAL MARKER DETECTION COMPLETE")
print("="*70)
print("\nNext: Analyze temporal_marker_density.csv to see clustering over time")