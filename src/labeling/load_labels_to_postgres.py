"""
load_labels_to_postgres.py - Load zero-shot labels from JSON into PostgreSQL
"""

import json
import psycopg2
from pathlib import Path


def load_labels(json_path: str = "data/zero_shot_labels.json"):
    """Load zero-shot labels into PostgreSQL"""
    
    # Load JSON
    with open(json_path) as f:
        results = json.load(f)
    
    print(f"Loaded {len(results)} labeled stories from JSON")
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="linguistic_predictor_v2",
        user="analyzer",
        password="dev_password_change_in_prod"
    )
    cursor = conn.cursor()
    
    inserted = 0
    skipped = 0
    
    for r in results:
        story_id = r['story_id']
        labels = r['labels']
        
        try:
            cursor.execute("""
                INSERT INTO zero_shot_labels (
                    story_id,
                    temporal_bleed, certainty_collapse, emotional_valence_shift,
                    agency_reversal, novel_meme_explosion, metaphor_cluster_density,
                    pronoun_flip, sacred_profane_ratio, time_compression_markers,
                    temporal_bleed_reason, certainty_collapse_reason, emotional_valence_shift_reason,
                    agency_reversal_reason, novel_meme_explosion_reason, metaphor_cluster_density_reason,
                    pronoun_flip_reason, sacred_profane_ratio_reason, time_compression_markers_reason
                ) VALUES (
                    %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (story_id) DO NOTHING
            """, (
                story_id,
                labels['temporal_bleed']['score'],
                labels['certainty_collapse']['score'],
                labels['emotional_valence_shift']['score'],
                labels['agency_reversal']['score'],
                labels['novel_meme_explosion']['score'],
                labels['metaphor_cluster_density']['score'],
                labels['pronoun_flip']['score'],
                labels['sacred_profane_ratio']['score'],
                labels['time_compression_markers']['score'],
                labels['temporal_bleed']['reason'],
                labels['certainty_collapse']['reason'],
                labels['emotional_valence_shift']['reason'],
                labels['agency_reversal']['reason'],
                labels['novel_meme_explosion']['reason'],
                labels['metaphor_cluster_density']['reason'],
                labels['pronoun_flip']['reason'],
                labels['sacred_profane_ratio']['reason'],
                labels['time_compression_markers']['reason']
            ))
            inserted += 1
        except Exception as e:
            print(f"Error on {story_id}: {e}")
            skipped += 1
        
        if inserted % 500 == 0 and inserted > 0:
            print(f"  Inserted {inserted}...")
            conn.commit()
    
    conn.commit()
    conn.close()
    
    print(f"\nDone! Inserted: {inserted}, Skipped: {skipped}")


if __name__ == "__main__":
    load_labels()