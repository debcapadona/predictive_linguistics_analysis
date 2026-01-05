"""
Validate dimension spikes during known major events
Compare to GCP-style anomaly detection
"""
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
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
    'Trump Assassination Attempt': {
        'date': '2024-07-13',
        'window_days': 3,  # Look at +/- 3 days
        'description': 'Trump shot at rally in Pennsylvania'
    },
    'Election Day 2024': {
        'date': '2024-11-05',
        'window_days': 3,
        'description': 'US Presidential Election'
    }
}

# Dimensions to analyze
dimensions = [
    'emotional_valence_shift',
    'temporal_bleed',
    'certainty_collapse',
    'time_compression',
    'agency_reversal'
]

results = []

for event_name, event_info in events.items():
    print(f"\n{'='*70}")
    print(f"Analyzing: {event_name}")
    print(f"Date: {event_info['date']}")
    print(f"{'='*70}")
    
    event_date = datetime.strptime(event_info['date'], '%Y-%m-%d')
    window = timedelta(days=event_info['window_days'])
    
    start_date = event_date - window
    end_date = event_date + window
    
    # Baseline: 30 days before the event window
    baseline_start = start_date - timedelta(days=30)
    baseline_end = start_date - timedelta(days=1)
    
    for dimension in dimensions:
        # Get event window scores
        query_event = f"""
            SELECT AVG(b.score) as avg_score, COUNT(*) as count
            FROM bert_{dimension} b
            JOIN stories s ON b.story_id = s.id
            WHERE s.created_at BETWEEN %s AND %s
        """
        
        event_df = pd.read_sql(query_event, conn, 
                               params=(start_date, end_date))
        
        # Get baseline scores
        baseline_df = pd.read_sql(query_event, conn,
                                 params=(baseline_start, baseline_end))
        
        if event_df['count'].values[0] > 0 and baseline_df['count'].values[0] > 0:
            event_score = event_df['avg_score'].values[0]
            baseline_score = baseline_df['avg_score'].values[0]
            
            # Calculate percent change
            pct_change = ((event_score - baseline_score) / baseline_score) * 100
            
            results.append({
                'event': event_name,
                'dimension': dimension,
                'baseline_score': baseline_score,
                'event_score': event_score,
                'pct_change': pct_change,
                'event_count': event_df['count'].values[0],
                'baseline_count': baseline_df['count'].values[0]
            })
            
            print(f"\n{dimension}:")
            print(f"  Baseline (30 days prior): {baseline_score:.4f} (n={baseline_df['count'].values[0]})")
            print(f"  Event window: {event_score:.4f} (n={event_df['count'].values[0]})")
            print(f"  Change: {pct_change:+.1f}%")
            
            if abs(pct_change) > 10:
                print(f"  ⚠️  SIGNIFICANT SPIKE!")

# Create visualization
results_df = pd.DataFrame(results)
results_df.to_csv('data/event_validation_results.csv', index=False)

print("\n" + "="*70)
print("SUMMARY")
print("="*70)

# Create heatmap of percent changes
fig, ax = plt.subplots(figsize=(10, 6))

pivot_data = results_df.pivot(index='dimension', columns='event', values='pct_change')

sns.heatmap(pivot_data, annot=True, fmt='.1f', cmap='RdYlGn_r', 
            center=0, cbar_kws={'label': 'Percent Change from Baseline'},
            linewidths=0.5, ax=ax, vmin=-50, vmax=50)

ax.set_title('Dimension Changes During Major Events\n(Compared to 30-Day Baseline)', 
             fontsize=14, pad=15)
ax.set_xlabel('Event', fontsize=12)
ax.set_ylabel('BERT Dimension', fontsize=12)

plt.tight_layout()
plt.savefig('visualizations/event_validation_heatmap.png', dpi=300, bbox_inches='tight')
print("\n✓ Saved: visualizations/event_validation_heatmap.png")

# Time series for each event
for event_name, event_info in events.items():
    print(f"\nCreating time series for {event_name}...")
    
    event_date = datetime.strptime(event_info['date'], '%Y-%m-%d')
    
    # Get daily averages for +/- 14 days
    start = event_date - timedelta(days=14)
    end = event_date + timedelta(days=14)
    
    fig, axes = plt.subplots(3, 2, figsize=(14, 10))
    axes = axes.flatten()
    
    for idx, dimension in enumerate(dimensions):
        query = f"""
            SELECT 
                DATE(created_at) as date,
                AVG(score) as avg_score,
                COUNT(*) as count
            FROM bert_{dimension}
            WHERE created_at BETWEEN %s AND %s
            GROUP BY DATE(created_at)
            ORDER BY date
        """
        
        df = pd.read_sql(query, conn, params=(start, end))
        
        if len(df) > 0:
            ax = axes[idx]
            ax.plot(pd.to_datetime(df['date']), df['avg_score'], 
                   marker='o', linewidth=2, markersize=6)
            ax.axvline(event_date, color='red', linestyle='--', 
                      linewidth=2, label='Event Date', alpha=0.7)
            ax.set_title(dimension.replace('_', ' ').title(), fontsize=11)
            ax.set_xlabel('Date', fontsize=9)
            ax.set_ylabel('Average Score', fontsize=9)
            ax.grid(True, alpha=0.3)
            ax.legend(fontsize=8)
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Remove extra subplot
    fig.delaxes(axes[5])
    
    plt.suptitle(f'{event_name} - Dimension Time Series\n({event_info["description"]})', 
                 fontsize=14)
    plt.tight_layout()
    
    filename = event_name.lower().replace(' ', '_')
    plt.savefig(f'visualizations/event_{filename}_timeseries.png', 
                dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: visualizations/event_{filename}_timeseries.png")

conn.close()

print("\n✓ Event validation complete!")
print("\nCheck visualizations/ for:")
print("  - event_validation_heatmap.png (summary)")
print("  - event_*_timeseries.png (daily trends)")
print("\nResults saved to data/event_validation_results.csv")