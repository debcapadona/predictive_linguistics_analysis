"""
Score words based on temporal context
Assigns temporal urgency score (0-1) to each word based on nearby temporal markers
"""
import psycopg2
import pandas as pd
from collections import defaultdict

print("="*70)
print("TEMPORAL CONTEXT SCORING")
print("="*70)

# Temporal markers with urgency scores (0.0 = distant, 1.0 = immediate)
TEMPORAL_SCORES = {
    # Immediate/urgent (0.9-1.0)
    'now': 1.0, 'right now': 1.0, 'urgent': 1.0, 'immediately': 1.0,
    'breaking': 1.0, 'emergency': 1.0, 'critical': 1.0, 'asap': 1.0,
    'currently': 0.95, 'today': 0.95, 'happening': 0.95, 'live': 0.95,
    
    # Very near term (0.7-0.9)
    'tomorrow': 0.85, 'tonight': 0.85, 'soon': 0.8, 'shortly': 0.8,
    'upcoming': 0.75, 'next': 0.75, 'imminent': 0.75, 'approaching': 0.75,
    
    # Near term (0.5-0.7)
    'week': 0.6, 'days': 0.6, 'coming': 0.6, 'recent': 0.6,
    'lately': 0.6, 'this month': 0.55,
    
    # Medium term (0.3-0.5)
    'months': 0.4, 'quarter': 0.4, 'season': 0.4, 'year': 0.35,
    
    # Distant (0.0-0.3)
    'years': 0.2, 'decade': 0.15, 'eventually': 0.2, 'someday': 0.1,
    'future': 0.25, 'long term': 0.15,
    
    # Change indicators (boost by 0.2)
    'suddenly': 0.3, 'unexpected': 0.3, 'surprise': 0.3, 'shock': 0.3,
}

print(f"\nTemporal vocabulary: {len(TEMPORAL_SCORES)} markers")

pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)

cur = pg_conn.cursor()

# Create word temporal scores table
print("\nCreating word_temporal_scores table...")
cur.execute("""
    CREATE TABLE IF NOT EXISTS word_temporal_scores (
        id SERIAL PRIMARY KEY,
        word TEXT NOT NULL,
        date DATE NOT NULL,
        avg_temporal_score FLOAT NOT NULL,
        occurrence_count INTEGER NOT NULL,
        with_temporal_context INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    )
""")

cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_word_temporal_word_date 
    ON word_temporal_scores(word, date)
""")

pg_conn.commit()
print("  ✓ Table created")

# Clear existing data
cur.execute("DELETE FROM word_temporal_scores")
pg_conn.commit()

# Process word tokens with context window
print("\nScoring words by temporal context...")
print("  Context window: ±5 words")

# Get all 2024 stories with their word sequences
query = """
    SELECT 
        wt.story_id,
        s.created_at,
        ARRAY_AGG(LOWER(wt.word_text) ORDER BY wt.position) as words,
        ARRAY_AGG(wt.position ORDER BY wt.position) as positions
    FROM word_tokens wt
    JOIN stories s ON wt.story_id = s.id
    WHERE DATE(s.created_at) BETWEEN '2024-01-01' AND '2024-11-30'
    GROUP BY wt.story_id, s.created_at
"""

print("  Loading word sequences (this takes a minute)...")
df = pd.read_sql(query, pg_conn)
print(f"  ✓ Loaded {len(df):,} stories")

# Track word scores by date
word_date_scores = defaultdict(lambda: {'scores': [], 'total_count': 0, 'with_context': 0})

CONTEXT_WINDOW = 5

for idx, row in df.iterrows():
    if idx % 1000 == 0:
        print(f"    Processing story {idx:,}/{len(df):,}")
    
    words = row['words']
    date = row['created_at'].date()
    
    # Score each word based on nearby temporal markers
    for i, word in enumerate(words):
        if len(word) <= 3:  # Skip short words
            continue
        
        # Look at context window (±5 words)
        context_start = max(0, i - CONTEXT_WINDOW)
        context_end = min(len(words), i + CONTEXT_WINDOW + 1)
        context = words[context_start:context_end]
        
        # Find temporal markers in context
        temporal_scores_found = []
        for context_word in context:
            if context_word in TEMPORAL_SCORES:
                temporal_scores_found.append(TEMPORAL_SCORES[context_word])
        
        # Calculate temporal score for this word occurrence
        if temporal_scores_found:
            temporal_score = max(temporal_scores_found)  # Use highest urgency in context
            word_date_scores[(word, date)]['scores'].append(temporal_score)
            word_date_scores[(word, date)]['with_context'] += 1
        
        word_date_scores[(word, date)]['total_count'] += 1

print(f"\n  ✓ Scored {len(word_date_scores):,} unique (word, date) pairs")

# Aggregate and save
print("\nAggregating temporal scores...")

batch = []
for (word, date), data in word_date_scores.items():
    if data['scores']:  # Only save words that had temporal context
        avg_score = sum(data['scores']) / len(data['scores'])
        
        batch.append((
            word,
            date,
            avg_score,
            data['total_count'],
            data['with_context']
        ))
    
    if len(batch) >= 1000:
        cur.executemany("""
            INSERT INTO word_temporal_scores 
            (word, date, avg_temporal_score, occurrence_count, with_temporal_context)
            VALUES (%s, %s, %s, %s, %s)
        """, batch)
        pg_conn.commit()
        batch = []

# Final batch
if batch:
    cur.executemany("""
        INSERT INTO word_temporal_scores 
        (word, date, avg_temporal_score, occurrence_count, with_temporal_context)
        VALUES (%s, %s, %s, %s, %s)
    """, batch)
    pg_conn.commit()

# Summary stats
cur.execute("SELECT COUNT(*) FROM word_temporal_scores")
total_rows = cur.fetchone()[0]
print(f"  ✓ Saved {total_rows:,} word-date temporal scores")

# Show examples
print("\nExample temporal scores (Aug 15-25):")
cur.execute("""
    SELECT 
        word,
        AVG(avg_temporal_score) as avg_score,
        SUM(occurrence_count) as total_occurrences,
        SUM(with_temporal_context) as with_context
    FROM word_temporal_scores
    WHERE date BETWEEN '2024-08-15' AND '2024-08-25'
    AND occurrence_count > 5
    GROUP BY word
    ORDER BY avg_score DESC
    LIMIT 20
""")

print(f"\n{'Word':<20} {'Temporal Score':<15} {'Occurrences':<12} {'With Context':<12}")
print(f"{'-'*20} {'-'*15} {'-'*12} {'-'*12}")

for word, score, occurrences, with_context in cur.fetchall():
    print(f"{word:<20} {score:>14.3f} {occurrences:>11} {with_context:>11}")

pg_conn.close()

print("\n" + "="*70)
print("TEMPORAL SCORING COMPLETE")
print("="*70)
print("\nNow you can join word burst analysis with temporal scores!")