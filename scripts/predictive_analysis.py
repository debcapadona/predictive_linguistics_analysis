"""
Analyze whether linguistic dimensions provided advance warning
of the Reddit API Blackout event
"""
import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns

print("="*70)
print("PREDICTIVE ANALYSIS: Reddit API Blackout")
print("Can we detect advance warning signals?")
print("="*70)

# Load daily time series
df = pd.read_csv('data/reddit_event_daily_timeseries.csv')
df['date'] = pd.to_datetime(df['date'])

event_date = pd.to_datetime('2024-06-12')

dimensions = df['dimension'].unique()

results = []

for dimension in dimensions:
    dim_data = df[df['dimension'] == dimension].copy()
    dim_data = dim_data.sort_values('date')
    
    # Define periods
    # Baseline: 14-7 days before event
    # Warning window: 7-1 days before event
    # Event: day of event
    
    baseline_start = event_date - pd.Timedelta(days=14)
    baseline_end = event_date - pd.Timedelta(days=7)
    
    warning_start = event_date - pd.Timedelta(days=7)
    warning_end = event_date - pd.Timedelta(days=1)
    
    baseline = dim_data[
        (dim_data['date'] >= baseline_start) & 
        (dim_data['date'] < baseline_end)
    ]
    
    warning = dim_data[
        (dim_data['date'] >= warning_start) & 
        (dim_data['date'] < event_date)
    ]
    
    event = dim_data[dim_data['date'] == event_date]
    
    if len(baseline) > 0 and len(warning) > 0 and len(event) > 0:
        baseline_mean = baseline['avg_score'].mean()
        warning_mean = warning['avg_score'].mean()
        event_score = event['avg_score'].values[0]
        
        # Test 1: Was warning period elevated vs baseline?
        if len(baseline) >= 2 and len(warning) >= 2:
            t_stat, p_val = stats.ttest_ind(
                warning['avg_score'].values,
                baseline['avg_score'].values
            )
        else:
            t_stat, p_val = 0, 1
        
        # Test 2: Linear trend in warning period
        warning_days = (warning['date'] - warning_start).dt.days.values
        if len(warning_days) >= 3:
            slope, intercept, r_value, p_trend, std_err = stats.linregress(
                warning_days,
                warning['avg_score'].values
            )
        else:
            slope, p_trend, r_value = 0, 1, 0
        
        # Test 3: Peak detection in warning window
        if len(warning) > 0:
            max_warning = warning['avg_score'].max()
            max_warning_date = warning[warning['avg_score'] == max_warning]['date'].values[0]
            days_before_event = (event_date - pd.to_datetime(max_warning_date)).days
        else:
            max_warning = 0
            days_before_event = 0
        
        results.append({
            'dimension': dimension,
            'baseline_mean': baseline_mean,
            'warning_mean': warning_mean,
            'event_score': event_score,
            'baseline_to_warning_change_pct': ((warning_mean - baseline_mean) / baseline_mean) * 100,
            'warning_elevated': warning_mean > baseline_mean,
            't_statistic': t_stat,
            'p_value': p_val,
            'significant_elevation': p_val < 0.05,
            'trend_slope': slope,
            'trend_p_value': p_trend,
            'trend_r_squared': r_value**2,
            'significant_uptrend': (slope > 0) and (p_trend < 0.10),
            'max_warning_score': max_warning,
            'days_before_peak': days_before_event,
            'peak_above_baseline': max_warning > baseline_mean * 1.1
        })

results_df = pd.DataFrame(results)
results_df = results_df.round(4)

print("\n1. ADVANCE WARNING DETECTION")
print("="*70)

for _, row in results_df.iterrows():
    print(f"\n{row['dimension']}:")
    print(f"  Baseline (14-7 days before): {row['baseline_mean']:.4f}")
    print(f"  Warning window (7-1 days before): {row['warning_mean']:.4f}")
    print(f"  Event day: {row['event_score']:.4f}")
    print(f"  Warning period change: {row['baseline_to_warning_change_pct']:+.1f}%")
    
    if row['significant_elevation']:
        print(f"  ✓ Warning period significantly elevated (p={row['p_value']:.4f})")
    
    if row['significant_uptrend']:
        print(f"  ✓ Significant upward trend detected (slope={row['trend_slope']:.6f}, p={row['trend_p_value']:.4f})")
    
    if row['peak_above_baseline']:
        print(f"  ✓ Peak {row['days_before_peak']} days before event (score={row['max_warning_score']:.4f})")

# Summary
print("\n" + "="*70)
print("PREDICTIVE SIGNAL SUMMARY")
print("="*70)

elevated = results_df[results_df['significant_elevation'] == True]
trending = results_df[results_df['significant_uptrend'] == True]
peaked = results_df[results_df['peak_above_baseline'] == True]

print(f"\nDimensions with elevated warning period: {len(elevated)}/{len(results_df)}")
for _, row in elevated.iterrows():
    print(f"  - {row['dimension']}: {row['baseline_to_warning_change_pct']:+.1f}%")

print(f"\nDimensions with upward trend: {len(trending)}/{len(results_df)}")
for _, row in trending.iterrows():
    print(f"  - {row['dimension']}")

print(f"\nDimensions with early peaks: {len(peaked)}/{len(results_df)}")
for _, row in peaked.iterrows():
    print(f"  - {row['dimension']}: peaked {row['days_before_peak']} days before event")

# Create visualization
fig, axes = plt.subplots(3, 2, figsize=(14, 12))
axes = axes.flatten()

for idx, dimension in enumerate(dimensions):
    if idx >= 6:
        break
    
    ax = axes[idx]
    dim_data = df[df['dimension'] == dimension].copy()
    dim_data = dim_data.sort_values('date')
    
    # Plot time series
    ax.plot(dim_data['date'], dim_data['avg_score'], 
           marker='o', linewidth=2, markersize=4, label='Daily average')
    
    # Mark event day
    ax.axvline(event_date, color='red', linestyle='--', 
              linewidth=2, label='Event day', alpha=0.7)
    
    # Mark warning period
    ax.axvspan(event_date - pd.Timedelta(days=7), 
              event_date - pd.Timedelta(days=1),
              alpha=0.2, color='orange', label='Warning window')
    
    # Mark baseline period
    ax.axvspan(event_date - pd.Timedelta(days=14), 
              event_date - pd.Timedelta(days=7),
              alpha=0.2, color='blue', label='Baseline')
    
    row = results_df[results_df['dimension'] == dimension].iloc[0]
    ax.axhline(row['baseline_mean'], color='blue', 
              linestyle=':', alpha=0.5, linewidth=1)
    
    ax.set_title(f"{dimension}\n(Warning: {row['baseline_to_warning_change_pct']:+.1f}%)", 
                fontsize=10)
    ax.set_xlabel('Date', fontsize=9)
    ax.set_ylabel('Score', fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis='x', rotation=45)
    
    if idx == 0:
        ax.legend(fontsize=7, loc='upper left')

plt.suptitle('Predictive Signal Analysis: Reddit API Blackout\nDid dimensions rise BEFORE the event?', 
            fontsize=14)
plt.tight_layout()
plt.savefig('visualizations/predictive_analysis_reddit.png', dpi=300, bbox_inches='tight')
print("\n✓ Saved: visualizations/predictive_analysis_reddit.png")

results_df.to_csv('data/predictive_analysis_results.csv', index=False)
print("✓ Saved: data/predictive_analysis_results.csv")

print("\n" + "="*70)
print("CONCLUSION")
print("="*70)

if len(elevated) > 0 or len(trending) > 0 or len(peaked) > 0:
    print("\n✓ YES - Predictive signals detected!")
    print(f"  {len(elevated)} dimensions showed elevated warning periods")
    print(f"  {len(trending)} dimensions showed upward trends")
    print(f"  {len(peaked)} dimensions peaked before event day")
else:
    print("\n✗ NO - No clear predictive signals found")
    print("  System detected event retrospectively, not predictively")