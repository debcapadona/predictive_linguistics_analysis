"""
Search ALL 2024 data for anticipatory signals about 2025 political violence
Looking for:
- Political violence vocabulary + future temporal markers
- Topics clustering around politics/violence/threat
- Temporal urgency building over time
"""
import psycopg2
import pandas as pd
from datetime import datetime

print("="*70)
print("ANTICIPATORY SIGNAL DETECTION - 2025 EVENTS")
print("="*70)

pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)

# Target vocabulary for political violence
POLITICAL_VIOLENCE_WORDS = [
    'assassination', 'assassinate', 'shooting', 'shooter', 'gunman',
    'violence', 'attack', 'threat', 'kill', 'death',
    'politician', 'politicians', 'lawmaker', 'lawmakers', 'congressman',
    'representative', 'senator', 'democracy', 'political',
    'campus', 'activist', 'activists', 'conservative', 'liberal',
    'polarization', 'polarized', 'civil war', 'conflict'
]

# Future temporal markers (not present/past)
FUTURE_TEMPORAL = [
    'will', 'next year', 'coming', '2025', 'soon', 'inevitable',
    'when not if', 'eventually', 'approaching', 'headed', 'going to',
    'expect', 'predicted', 'forecast', 'anticipate', 'looming'
]

print(f"\nTarget vocabulary:")
print(f"  Political violence: {len(POLITICAL_VIOLENCE_WORDS)} words")
print(f"  Future temporal: {len(FUTURE_TEMPORAL)} terms")

# === 1. Find stories with political violence + future temporal markers ===
print("\n" + "="*70)
print("1. STORIES WITH POLITICAL VIOLENCE + FUTURE TEMPORAL")
print("="*70)

query = """
    WITH political_stories AS (
        SELECT DISTINCT
            wt.story_id,
            s.created_at,
            s.title
        FROM word_tokens wt
        JOIN stories s ON wt.story_id = s.id
        WHERE LOWER(wt.word_text) IN ({political_words})
        AND DATE(s.created_at) BETWEEN '2024-01-01' AND '2024-12-31'
    ),
    future_stories AS (
        SELECT DISTINCT
            wt.story_id
        FROM word_tokens wt
        WHERE LOWER(wt.word_text) IN ({future_words})
    ),
    combined AS (
        SELECT 
            p.story_id,
            p.created_at,
            p.title,
            DATE(p.created_at) as date
        FROM political_stories p
        JOIN future_stories f ON p.story_id = f.story_id
    )
    SELECT 
        date,
        COUNT(*) as story_count,
        STRING_AGG(DISTINCT title, ' | ') as sample_titles
    FROM combined
    GROUP BY date
    ORDER BY date
""".format(
    political_words=','.join([f"'{w}'" for w in POLITICAL_VIOLENCE_WORDS]),
    future_words=','.join([f"'{w}'" for w in FUTURE_TEMPORAL])
)

print("\nQuerying stories (this may take a minute)...")
df_stories = pd.read_sql(query, pg_conn)
df_stories['date'] = pd.to_datetime(df_stories['date'])

print(f"\n✓ Found {len(df_stories)} days with matching stories")
print(f"  Total stories: {df_stories['story_count'].sum():,}")

# Show monthly aggregation
print("\nMonthly distribution:")
df_stories['month'] = df_stories['date'].dt.to_period('M')
monthly = df_stories.groupby('month')['story_count'].sum()
for month, count in monthly.items():
    print(f"  {month}: {count:4d} stories")

# === 2. Get word co-occurrence patterns ===
print("\n" + "="*70)
print("2. WORD CO-OCCURRENCE ANALYSIS")
print("="*70)

# Find which political violence words appear with which temporal markers
cooccurrence_query = """
    WITH story_contexts AS (
        SELECT 
            wt1.story_id,
            s.created_at,
            wt1.word_text as political_word,
            wt2.word_text as temporal_word
        FROM word_tokens wt1
        JOIN word_tokens wt2 ON wt1.story_id = wt2.story_id
        JOIN stories s ON wt1.story_id = s.id
        WHERE LOWER(wt1.word_text) IN ({political_words})
        AND LOWER(wt2.word_text) IN ({future_words})
        AND DATE(s.created_at) BETWEEN '2024-01-01' AND '2024-12-31'
    )
    SELECT 
        LOWER(political_word) as political_word,
        LOWER(temporal_word) as temporal_word,
        COUNT(*) as cooccurrence_count,
        COUNT(DISTINCT story_id) as unique_stories
    FROM story_contexts
    GROUP BY LOWER(political_word), LOWER(temporal_word)
    ORDER BY cooccurrence_count DESC
    LIMIT 50
""".format(
    political_words=','.join([f"'{w}'" for w in POLITICAL_VIOLENCE_WORDS]),
    future_words=','.join([f"'{w}'" for w in FUTURE_TEMPORAL])
)

print("\nFinding word co-occurrences...")
df_cooccur = pd.read_sql(cooccurrence_query, pg_conn)

print(f"\nTop 30 Political Violence + Future Temporal pairs:")
print(f"{'Political Word':<20} {'Temporal Word':<15} {'Stories':<10} {'Total':<10}")
print("-" * 60)
for _, row in df_cooccur.head(30).iterrows():
    print(f"{row['political_word']:<20} {row['temporal_word']:<15} {row['unique_stories']:<10} {row['cooccurrence_count']:<10}")

# === 3. Check dimensional coherence during high-signal periods ===
print("\n" + "="*70)
print("3. DIMENSIONAL COHERENCE DURING HIGH-SIGNAL DAYS")
print("="*70)

# Get top 10 days with most political violence + future temporal stories
top_dates = df_stories.nlargest(10, 'story_count')['date'].dt.date.tolist()

print("\nTop 10 days with highest signal:")
for date in top_dates:
    count = df_stories[df_stories['date'].dt.date == date]['story_count'].values[0]
    print(f"  {date}: {count} stories")

# Load Event Coherence Index for these dates
eci_df = pd.read_csv('data/event_coherence_index.csv')
eci_df['date'] = pd.to_datetime(eci_df['date'])

print("\nEvent Coherence scores on high-signal days:")
for date in top_dates:
    eci_row = eci_df[eci_df['date'].dt.date == date]
    if not eci_row.empty:
        eci = eci_row['event_coherence_index'].values[0]
        print(f"  {date}: ECI = {eci:.3f}")

# === 4. Time series of anticipatory signal ===
print("\n" + "="*70)
print("4. CREATING TIME SERIES")
print("="*70)

# Save detailed results
df_stories.to_csv('data/anticipatory_signals_2025.csv', index=False)
df_cooccur.to_csv('data/word_cooccurrence_political_violence.csv', index=False)

print("\n✓ Saved:")
print("  - data/anticipatory_signals_2025.csv")
print("  - data/word_cooccurrence_political_violence.csv")

# === 5. Sample titles from key periods ===
print("\n" + "="*70)
print("5. SAMPLE STORY TITLES FROM HIGH-SIGNAL PERIODS")
print("="*70)

for date in top_dates[:5]:
    date_str = date.strftime('%Y-%m-%d')
    titles = df_stories[df_stories['date'].dt.date == date]['sample_titles'].values
    if len(titles) > 0:
        print(f"\n{date_str}:")
        # Split titles and show first 3
        title_list = str(titles[0]).split(' | ')[:3]
        for title in title_list:
            print(f"  • {title}")

pg_conn.close()

print("\n" + "="*70)
print("ANALYSIS COMPLETE")
print("="*70)
print("\nNext: Visualize anticipatory signal timeline")