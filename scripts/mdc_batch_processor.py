"""
MDC Batch Processor
Process stories through MDC orchestrator and save to PostgreSQL
"""

import sys
from pathlib import Path
from datetime import datetime
import time

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.analyzers.mdc.mdc_main import MDCOrchestrator
from src.core.database import Database


def process_stories(start_date: str, end_date: str, use_llm: bool = False, limit: int = None):
    """
    Process stories through MDC and save to PostgreSQL
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        use_llm: Whether to use LLM for temporal bleed (costs money)
        limit: Optional limit on number of stories
    """
    # Initialize
    orchestrator = MDCOrchestrator()
    
    # SQLite for reading stories
    sqlite_db = Database(db_type='sqlite', db_path='data/linguistic_predictor.db')
    
    # PostgreSQL for writing classifications
    postgres_db = Database(
        db_type='postgresql',
        host='localhost',
        port=5432,
        database='linguistic_predictor_v2',
        user='analyzer',
        password='dev_password_change_in_prod'
    )
    
    print(f"MDC Batch Processor")
    print(f"Date range: {start_date} to {end_date}")
    print(f"LLM enabled: {use_llm}")
    print(f"Limit: {limit if limit else 'None'}")
    print("=" * 80)
    
    # Get stories from SQLite
    query = """
        SELECT id, title, created_at
        FROM stories
        WHERE created_at >= ? AND created_at < ?
        ORDER BY created_at
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    stories = sqlite_db.execute_query(query, (start_date, end_date))
    total_stories = len(stories)
    
    print(f"\nFound {total_stories} stories to process")
    
    if total_stories == 0:
        print("No stories found in date range. Exiting.")
        return
    
    # Process stories
    processed = 0
    skipped = 0
    errors = 0
    start_time = time.time()
    
    for story in stories:
        story_id, title, created_at = story
        
        try:
            # Check if already processed
            existing = postgres_db.execute_query(
                "SELECT story_id FROM story_classifications WHERE story_id = %s",
                (story_id,)
            )
            
            if existing:
                skipped += 1
                if skipped % 100 == 0:
                    print(f"  Skipped {skipped} already-processed stories...")
                continue
            
            # Run MDC classification
            results = orchestrator.vectorize_story(title, use_llm=use_llm)
            
            # Get or create classification
            classification_id = postgres_db.get_or_create_classification(
                certainty_score=results['certainty_score'],
                pronoun_first=results['pronoun_first'],
                pronoun_collective=results['pronoun_collective'],
                valence_score=results['valence_score'],
                temporal_bleed=results['temporal_bleed'],
                time_compression=results['time_compression'],
                sacred_profane=results['sacred_profane'],
                temporal_proximity=results['temporal_proximity']
            )
            
            # Link story to classification
            postgres_db.add_story_classification(story_id, classification_id)
            
            processed += 1
            
            # Progress update
            if processed % 100 == 0:
                elapsed = time.time() - start_time
                rate = processed / elapsed
                remaining = (total_stories - processed - skipped) / rate if rate > 0 else 0
                print(f"  Processed: {processed}/{total_stories} ({processed/total_stories*100:.1f}%) | "
                      f"Rate: {rate:.1f}/sec | ETA: {remaining/60:.1f} min")
        
        except Exception as e:
            errors += 1
            print(f"  ERROR processing story {story_id}: {str(e)}")
            # Rollback failed transaction so we can continue
            postgres_db.conn.rollback()
            if errors > 10:
                print(f"  Too many errors ({errors}), stopping.")
                break
    
    # Final report
    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print(f"Processing complete!")
    print(f"  Processed: {processed}")
    print(f"  Skipped: {skipped}")
    print(f"  Errors: {errors}")
    print(f"  Time: {elapsed/60:.1f} minutes")
    print(f"  Rate: {processed/elapsed:.1f} stories/sec")
    
    # Show unique classifications
    unique_classifications = postgres_db.execute_query(
        "SELECT COUNT(*) FROM mdc_classifications"
    )
    print(f"  Unique classifications: {unique_classifications[0][0]}")
    
    postgres_db.close()
    sqlite_db.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process stories through MDC')
    parser.add_argument('start_date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('end_date', help='End date (YYYY-MM-DD)')
    parser.add_argument('--llm', action='store_true', help='Use LLM for temporal bleed')
    parser.add_argument('--limit', type=int, help='Limit number of stories')
    
    args = parser.parse_args()
    
    process_stories(args.start_date, args.end_date, use_llm=args.llm, limit=args.limit)