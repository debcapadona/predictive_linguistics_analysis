"""
Export event analysis to clean CSV for review
"""
import pandas as pd
import psycopg2
from datetime import datetime, timedelta

print("Connecting to database...")
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='linguistic_predictor_v2',
    user='analyzer',
    password='dev_password_change_in_prod'
)

# Define test events
events = {
    'Reddit API Blackout': {
        'date': '2024-06-12',
        'window_days': 3,
        'description': 'Reddit third-party app shutdown'
    },
    'Trump Assassination Attempt': {
        'date': '2024-07-13',
        'window_days': 3,
        'description': 'Trump shot at Pennsylvania rally'
    },
    'CrowdStrike Outage': {
        'date': '2024-07-19',
        'window_days': 2,
        'description': 'Global IT systems crash'
    },
    'Election Day 2024': {
        'date': '2024-11-05',
        'window_days': 3,
        'description': 'US Presidential Election'
    }
}

dimensions = [
    'emotional_valence_shift',
    'temporal_bleed',
    'certainty_collapse',
    'time_compression',
    'agency_reversal',
    'metaphor_cluster_density',
    'novel_meme_explosion',
    'sacred_profane_ratio',
    'pronoun_flip'
]

results = []

for event_name, event_info in events.items():
    print(f"Analyzing {event_name}...")
    
    event_date = datetime.strptime(event_info['date'], '%Y-%m-%d')
    window = timedelta(days=event_info['window_days'])
    
    start_date = event_date - window
    end_date = event_date + window
    
    # Baseline: 30 days before the event window
    baseline_start = start_date - timedelta(days=30)
    baseline_end = start_date - timedelta(days=1)
    
    for dimension in dimensions:
        query = f"""
            SELECT AVG(b.score) as avg_score, COUNT(*) as count
            FROM bert_{dimension} b
            JOIN stories s ON b.story_id = s.id
            WHERE s.created_at BETWEEN %s AND %s
        """
        
        event_df = pd.read_sql(query, conn, params=(start_date, end_date))
        baseline_df = pd.read_sql(query, conn, params=(baseline_start, baseline_end))
        
        if event_df['count'].values[0] > 0 and baseline_df['count'].values[0] > 0:
            event_score = event_df['avg_score'].values[0]
            baseline_score = baseline_df['avg_score'].values[0]
            pct_change = ((event_score - baseline_score) / baseline_score) * 100
            
            # Determine significance
            significance = 'None'
            if abs(pct_change) > 20:
                significance = 'High'
            elif abs(pct_change) > 10:
                significance = 'Medium'
            elif abs(pct_change) > 5:
                significance = 'Low'
            
            results.append({
                'Event': event_name,
                'Event Date': event_info['date'],
                'Description': event_info['description'],
                'Dimension': dimension,
                'Baseline Score': round(baseline_score, 4),
                'Event Score': round(event_score, 4),
                'Percent Change': round(pct_change, 2),
                'Direction': 'Increase' if pct_change > 0 else 'Decrease',
                'Significance': significance,
                'Baseline Sample Size': baseline_df['count'].values[0],
                'Event Sample Size': event_df['count'].values[0]
            })

results_df = pd.DataFrame(results)

# Save full results
results_df.to_csv('data/event_analysis_full.csv', index=False)
print(f"\n✓ Saved full results: data/event_analysis_full.csv")

# Create summary pivot
summary = results_df.pivot_table(
    index='Dimension',
    columns='Event',
    values='Percent Change',
    aggfunc='mean'
).round(2)

summary.to_csv('data/event_analysis_summary.csv')
print(f"✓ Saved summary pivot: data/event_analysis_summary.csv")

# Create significance summary
sig_events = results_df[results_df['Significance'].isin(['Medium', 'High'])]
sig_events = sig_events.sort_values(['Event', 'Percent Change'], ascending=[True, False])
sig_events.to_csv('data/event_analysis_significant.csv', index=False)
print(f"✓ Saved significant changes only: data/event_analysis_significant.csv")

print("\n" + "="*70)
print("SUMMARY OF SIGNIFICANT CHANGES (>10%)")
print("="*70)
print(f"\nTotal significant changes found: {len(sig_events)}")

for event_name in events.keys():
    event_sigs = sig_events[sig_events['Event'] == event_name]
    if len(event_sigs) > 0:
        print(f"\n{event_name}:")
        for _, row in event_sigs.iterrows():
            direction = "↑" if row['Direction'] == 'Increase' else "↓"
            print(f"  {direction} {row['Dimension']}: {row['Percent Change']:+.1f}% ({row['Significance']})")

conn.close()

print("\n✓ Done! Open these files in Google Sheets:")
print("  - data/event_analysis_full.csv (complete data)")
print("  - data/event_analysis_summary.csv (pivot table)")
print("  - data/event_analysis_significant.csv (notable changes only)")