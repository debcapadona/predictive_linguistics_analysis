"""
Build 3-tier topic taxonomy from BERTopic discoveries
Uses Claude API to intelligently organize topics into hierarchy
"""
import pandas as pd
import psycopg2
import anthropic
import os
import json
from datetime import datetime

# Load discovered topics
topics_df = pd.read_csv('data/discovered_topics.csv')
print(f"Loaded {len(topics_df)} topics")

# Prepare topic descriptions for Claude
topic_list = []
for _, row in topics_df.iterrows():
    if row['Topic'] == -1:  # Skip outlier topic
        continue
    
    # Get top 5 representative words
    words = eval(row['Representation'])[:5]  # Convert string to list
    topic_list.append({
        'topic_id': int(row['Topic']),
        'name': row['Name'],
        'keywords': words,
        'count': int(row['Count'])
    })

print(f"\nAnalyzing {len(topic_list)} topics with Claude...")

# Use Claude to organize into 3-tier taxonomy
from dotenv import load_dotenv
load_dotenv()

api_key = os.environ.get("ANTHROPIC_API_KEY")
print(f"API key loaded: {api_key[:20]}..." if api_key else "API key NOT found")

client = anthropic.Anthropic(api_key=api_key)

prompt = f"""Analyze these {len(topic_list)} discovered topics from Hacker News discussions and organize them into a 3-tier hierarchical taxonomy.

Topics discovered:
{json.dumps(topic_list, indent=2)}

Create a taxonomy with:
- **Tier 1**: 5-8 broad domains (e.g., Technology, Business, Science, Society, Culture)
- **Tier 2**: 15-25 mid-level categories under Tier 1 (e.g., under Technology: Programming, Hardware, Web)
- **Tier 3**: Map each discovered topic to the most appropriate Tier 2 category

Return ONLY valid JSON in this exact format:
{{
  "tier1": [
    {{
      "name": "Technology",
      "tier2": [
        {{
          "name": "Programming Languages",
          "tier3_topics": [9, 17]
        }},
        {{
          "name": "Hardware",
          "tier3_topics": [13]
        }}
      ]
    }}
  ]
}}

Rules:
- Use clear, concise names
- Every discovered topic must be assigned to exactly one Tier 3 slot
- Tier 2 categories should have 2-5 topics each
- Focus on actual HN discussion themes
"""

message = client.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=4000,
    messages=[{"role": "user", "content": prompt}]
)

response_text = message.content[0].text
print("\nClaude's taxonomy proposal:")
print(response_text[:500] + "...")

# Strip markdown code fences if present
if response_text.strip().startswith('```'):
    # Remove ```json and ``` markers
    response_text = response_text.strip()
    response_text = response_text.replace('```json', '').replace('```', '')
    response_text = response_text.strip()

# Parse JSON response
taxonomy = json.loads(response_text)

# Connect to database
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='linguistic_predictor_v2',
    user='analyzer',
    password='dev_password_change_in_prod'
)
cur = conn.cursor()

print("\nPopulating topic_taxonomy table...")

tier1_count = 0
tier2_count = 0
tier3_count = 0

# Insert taxonomy
for tier1_item in taxonomy['tier1']:
    # Insert Tier 1
    cur.execute("""
        INSERT INTO topic_taxonomy (topic_name, tier, parent_id)
        VALUES (%s, 1, NULL)
        RETURNING id
    """, (tier1_item['name'],))
    tier1_id = cur.fetchone()[0]
    tier1_count += 1
    
    for tier2_item in tier1_item['tier2']:
        # Insert Tier 2
        cur.execute("""
            INSERT INTO topic_taxonomy (topic_name, tier, parent_id)
            VALUES (%s, 2, %s)
            RETURNING id
        """, (tier2_item['name'], tier1_id))
        tier2_id = cur.fetchone()[0]
        tier2_count += 1
        
        # Insert Tier 3 (actual discovered topics)
        for topic_id in tier2_item['tier3_topics']:
            # Find topic name from original data
            topic_row = topics_df[topics_df['Topic'] == topic_id].iloc[0]
            topic_name = topic_row['Name']
            
            cur.execute("""
                INSERT INTO topic_taxonomy (topic_name, tier, parent_id)
                VALUES (%s, 3, %s)
            """, (f"Topic_{topic_id}: {topic_name}", tier2_id))
            tier3_count += 1

conn.commit()

print(f"\n✓ Taxonomy populated:")
print(f"  Tier 1 (domains): {tier1_count}")
print(f"  Tier 2 (categories): {tier2_count}")
print(f"  Tier 3 (specific topics): {tier3_count}")

# Save taxonomy structure
with open('data/taxonomy_structure.json', 'w') as f:
    json.dump(taxonomy, f, indent=2)
print(f"\n✓ Saved to data/taxonomy_structure.json")

# Show sample
print("\nSample taxonomy structure:")
cur.execute("""
    SELECT 
        t1.topic_name as tier1,
        t2.topic_name as tier2,
        t3.topic_name as tier3
    FROM topic_taxonomy t3
    JOIN topic_taxonomy t2 ON t3.parent_id = t2.id
    JOIN topic_taxonomy t1 ON t2.parent_id = t1.id
    WHERE t3.tier = 3
    LIMIT 10
""")

for row in cur.fetchall():
    print(f"  {row[0]} → {row[1]} → {row[2]}")

cur.close()
conn.close()

print("\n✓ Done! Taxonomy ready for labeling.")