"""
Analyze topic overlap between HN and Reddit BEFORE events
Find which topics both platforms discuss in predictive window
"""
import pandas as pd
import numpy as np
import sqlite3
import psycopg2
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

print("="*70)
print("CROSS-PLATFORM TOPIC OVERLAP ANALYSIS (PREDICTIVE)")
print("="*70)

# Events
events = [
    {'name': 'Reddit API Blackout', 'date': '2024-06-12'},
    {'name': 'Trump Assassination', 'date': '2024-07-13'},
    {'name': 'CrowdStrike Outage', 'date': '2024-07-19'},
    {'name': 'Election Day', 'date': '2024-11-05'}
]

# Connect to databases
reddit_conn = sqlite3.connect('data/reddit_snapshot_dec29.db')
pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)

# Get topic names from taxonomy
pg_cur = pg_conn.cursor()
pg_cur.execute("SELECT id, topic_name FROM topic_taxonomy WHERE tier = 3")
topic_map = {row[0]: row[1] for row in pg_cur.fetchall()}

results = []

for event in events:
    event_name = event['name']
    event_date = datetime.strptime(event['date'], '%Y-%m-%d')
    
    # Look 7 days BEFORE event (predictive window)
    predict_start = event_date - timedelta(days=7)
    predict_end = event_date - timedelta(days=1)
    
    print(f"\n{'='*70}")
    print(f"{event_name} - {event_date.date()}")
    print(f"Predictive window: {predict_start.date()} to {predict_end.date()}")
    print(f"{'='*70}")
    
    # Get HN top topics BEFORE event
    hn_query = """
        SELECT 
            cl.topic_id,
            COUNT(*) as count,
            AVG(cl.confidence) as avg_confidence
        FROM comment_labels cl
        JOIN comments c ON cl.comment_id::text = c.id
        JOIN stories s ON c.story_id = s.id
        WHERE DATE(s.created_at) BETWEEN %s AND %s
        AND cl.label_type = 'topic'
        GROUP BY cl.topic_id
        ORDER BY count DESC
        LIMIT 20
    """
    hn_topics = pd.read_sql(hn_query, pg_conn, params=(predict_start, predict_end))
    hn_topics['topic_name'] = hn_topics['topic_id'].map(topic_map)
    hn_topics['platform'] = 'HN'
    
    # Get Reddit top topics BEFORE event
    reddit_query = """
        SELECT 
            cl.topic_id,
            COUNT(*) as count,
            AVG(cl.confidence) as avg_confidence
        FROM reddit_comment_labels cl
        JOIN reddit_comments c ON cl.comment_id = c.id
        WHERE DATE(datetime(c.created_utc, 'unixepoch')) BETWEEN ? AND ?
        AND cl.label_type = 'topic'
        GROUP BY cl.topic_id
        ORDER BY count DESC
        LIMIT 20
    """
    reddit_topics = pd.read_sql(reddit_query, reddit_conn, 
                                params=(predict_start.strftime('%Y-%m-%d'), 
                                       predict_end.strftime('%Y-%m-%d')))
    reddit_topics['topic_name'] = reddit_topics['topic_id'].map(topic_map)
    reddit_topics['platform'] = 'Reddit'
    
    # Find overlapping topics
    hn_topic_ids = set(hn_topics['topic_id'].values)
    reddit_topic_ids = set(reddit_topics['topic_id'].values)
    overlap = hn_topic_ids.intersection(reddit_topic_ids)
    
    print(f"\nHN top topics (week before): {len(hn_topics)}")
    print(f"Reddit top topics (week before): {len(reddit_topics)}")
    print(f"Overlap: {len(overlap)} topics\n")
    
    if overlap:
        print("Shared topics (both platforms discussing before event):")
        for topic_id in sorted(overlap):
            topic_name = topic_map.get(topic_id, f"Topic_{topic_id}")
            hn_count = hn_topics[hn_topics['topic_id'] == topic_id]['count'].values[0]
            reddit_count = reddit_topics[reddit_topics['topic_id'] == topic_id]['count'].values[0]
            print(f"  • {topic_name:40s} | HN: {hn_count:5d} | Reddit: {reddit_count:6d}")
            
            results.append({
                'event': event_name,
                'topic_id': topic_id,
                'topic_name': topic_name,
                'hn_count': hn_count,
                'reddit_count': reddit_count,
                'shared': True
            })
    else:
        print("No overlapping topics found")
    
    # Top topics unique to each platform
    print(f"\nTop HN-only topics (before event):")
    hn_only = hn_topic_ids - reddit_topic_ids
    for topic_id in list(hn_only)[:5]:
        topic_name = topic_map.get(topic_id, f"Topic_{topic_id}")
        count = hn_topics[hn_topics['topic_id'] == topic_id]['count'].values[0]
        print(f"  • {topic_name:40s} | {count:5d} comments")
    
    print(f"\nTop Reddit-only topics (before event):")
    reddit_only = reddit_topic_ids - hn_topic_ids
    for topic_id in list(reddit_only)[:5]:
        topic_name = topic_map.get(topic_id, f"Topic_{topic_id}")
        count = reddit_topics[reddit_topics['topic_id'] == topic_id]['count'].values[0]
        print(f"  • {topic_name:40s} | {count:6d} comments")

# Save results
if results:
    results_df = pd.DataFrame(results)
    results_df.to_csv('data/topic_overlap_analysis.csv', index=False)
    print(f"\n✓ Saved: data/topic_overlap_analysis.csv")

reddit_conn.close()
pg_conn.close()

print("\n" + "="*70)
print("TOPIC OVERLAP ANALYSIS COMPLETE")
print("="*70)