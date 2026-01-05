"""
Analyze which topics spiked during the certainty_collapse anomaly
on June 10, 2024 (2 days before Reddit blackout)
"""
import pandas as pd
import psycopg2
from datetime import datetime

print("="*70)
print("TOPIC ANALYSIS: June 10, 2024 Certainty Collapse Anomaly")
print("="*70)

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='linguistic_predictor_v2',
    user='analyzer',
    password='dev_password_change_in_prod'
)

# Get topics discussed on June 10
anomaly_date = datetime(2024, 6, 10)

query = """
    SELECT 
        t1.topic_name as domain,
        t2.topic_name as category,
        t3.topic_name as topic,
        COUNT(DISTINCT cl.comment_id) as comment_count,
        AVG(cl.confidence) as avg_confidence
    FROM comment_labels cl
    JOIN stories s ON cl.comment_id::text = s.id
    JOIN topic_taxonomy t3 ON cl.topic_id = t3.id
    JOIN topic_taxonomy t2 ON t3.parent_id = t2.id
    JOIN topic_taxonomy t1 ON t2.parent_id = t1.id
    WHERE DATE(s.created_at) = %s
    AND cl.label_type = 'topic'
    AND t3.tier = 3
    GROUP BY t1.topic_name, t2.topic_name, t3.topic_name
    ORDER BY comment_count DESC
"""

june10_topics = pd.read_sql(query, conn, params=(anomaly_date,))

print(f"\nTopics discussed on June 10, 2024:")
print(f"Total topics: {len(june10_topics)}")
print("\nTop 20 topics by volume:")

for idx, row in june10_topics.head(20).iterrows():
    print(f"  {row['comment_count']:3d} comments - {row['topic'][:70]}")

# Now compare to baseline period (same day of week, different weeks)
# June 10 was a Monday, let's get other Mondays in May-July

baseline_query = """
    SELECT 
        t1.topic_name as domain,
        t2.topic_name as category,
        t3.topic_name as topic,
        COUNT(DISTINCT cl.comment_id) as comment_count
    FROM comment_labels cl
    JOIN stories s ON cl.comment_id::text = s.id
    JOIN topic_taxonomy t3 ON cl.topic_id = t3.id
    JOIN topic_taxonomy t2 ON t3.parent_id = t2.id
    JOIN topic_taxonomy t1 ON t2.parent_id = t1.id
    WHERE EXTRACT(DOW FROM s.created_at) = 1  -- Monday
    AND s.created_at BETWEEN '2024-05-01' AND '2024-07-31'
    AND DATE(s.created_at) != %s  -- Exclude June 10
    AND cl.label_type = 'topic'
    AND t3.tier = 3
    GROUP BY t1.topic_name, t2.topic_name, t3.topic_name
"""

baseline_topics = pd.read_sql(baseline_query, conn, params=(anomaly_date,))

# Calculate average Monday baseline per topic
baseline_avg = baseline_topics.groupby('topic')['comment_count'].mean().reset_index()
baseline_avg.columns = ['topic', 'baseline_avg']

# Merge with June 10 data
comparison = june10_topics.merge(baseline_avg, on='topic', how='left')
comparison['baseline_avg'] = comparison['baseline_avg'].fillna(0)
comparison['spike_ratio'] = comparison['comment_count'] / (comparison['baseline_avg'] + 1)
comparison = comparison.sort_values('spike_ratio', ascending=False)

print("\n" + "="*70)
print("ANOMALOUS TOPICS (Biggest spikes vs typical Monday)")
print("="*70)

print("\nTopics with >3x normal activity:")
spikes = comparison[comparison['spike_ratio'] > 3]

for idx, row in spikes.head(15).iterrows():
    print(f"\n{row['topic'][:70]}")
    print(f"  June 10: {row['comment_count']} comments")
    print(f"  Baseline Monday: {row['baseline_avg']:.1f} comments")
    print(f"  Spike ratio: {row['spike_ratio']:.1f}x")
    print(f"  Domain: {row['domain']}")

# Get actual story titles from June 10
print("\n" + "="*70)
print("TOP STORIES on June 10, 2024")
print("="*70)

stories_query = """
    SELECT 
        s.title,
        s.url,
        COUNT(DISTINCT cl.comment_id) as comment_count
    FROM stories s
    LEFT JOIN comment_labels cl ON s.id = cl.comment_id::text
    WHERE DATE(s.created_at) = %s
    AND s.content_type = 'header'
    GROUP BY s.id, s.title, s.url
    ORDER BY comment_count DESC
    LIMIT 20
"""

stories = pd.read_sql(stories_query, conn, params=(anomaly_date,))

print("\nMost-discussed stories:")
for idx, row in stories.iterrows():
    if pd.notna(row['title']):
        print(f"\n{idx+1}. {row['title']}")
        print(f"   Comments: {row['comment_count']}")
        if pd.notna(row['url']):
            print(f"   URL: {row['url'][:80]}")

# Check if Reddit-related topics appeared
print("\n" + "="*70)
print("REDDIT-RELATED SIGNALS")
print("="*70)

reddit_keywords = ['reddit', 'api', 'apollo', 'third-party', 'app']

reddit_stories = stories[
    stories['title'].str.lower().str.contains('|'.join(reddit_keywords), na=False)
]

if len(reddit_stories) > 0:
    print(f"\n✓ Found {len(reddit_stories)} Reddit-related stories on June 10:")
    for idx, row in reddit_stories.iterrows():
        print(f"  - {row['title']}")
        print(f"    Comments: {row['comment_count']}")
else:
    print("\n✗ No obvious Reddit-related stories in top 20")
    print("  (The signal may be implicit in general tech discussions)")

conn.close()

# Save results
comparison.to_csv('data/june10_topic_spikes.csv', index=False)
stories.to_csv('data/june10_top_stories.csv', index=False)

print("\n✓ Saved:")
print("  - data/june10_topic_spikes.csv")
print("  - data/june10_top_stories.csv")