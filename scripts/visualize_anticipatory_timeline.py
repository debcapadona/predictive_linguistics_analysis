"""
Visualize anticipatory signal timeline across ALL 2024
Show if political violence + future temporal language builds over time
"""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

print("Creating anticipatory signal timeline...")

# Load the data
df = pd.read_csv('data/anticipatory_signals_2025.csv')
df['date'] = pd.to_datetime(df['date'])

# Create figure
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(24, 12), sharex=True)

# Top panel: Daily story count
ax1.bar(df['date'], df['story_count'], width=1, alpha=0.7, color='darkred')
ax1.plot(df['date'], df['story_count'].rolling(7, center=True).mean(),
        linewidth=3, color='red', label='7-day moving average')

ax1.set_ylabel('Stories per Day\n(Political Violence + Future Temporal)', 
              fontsize=12, fontweight='bold')
ax1.set_title('Anticipatory Signal Detection: Political Violence Language in 2024 HN',
             fontsize=16, fontweight='bold', pad=15)
ax1.legend(loc='upper left', fontsize=11)
ax1.grid(True, alpha=0.3, axis='y')

# Bottom panel: Cumulative signal
df['cumulative'] = df['story_count'].cumsum()
ax2.plot(df['date'], df['cumulative'], linewidth=3, color='darkred')
ax2.fill_between(df['date'], 0, df['cumulative'], alpha=0.3, color='red')

ax2.set_xlabel('Date (2024)', fontsize=12, fontweight='bold')
ax2.set_ylabel('Cumulative Stories', fontsize=12, fontweight='bold')
ax2.grid(True, alpha=0.3)

# Format x-axis
ax2.xaxis.set_major_locator(mdates.MonthLocator())
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=0, ha='center', fontsize=11)

plt.tight_layout()
plt.savefig('visualizations/anticipatory_signals_2025.png', dpi=300, bbox_inches='tight')
print("✓ Saved: visualizations/anticipatory_signals_2025.png")

# Print statistics
print(f"\nTotal 2024 stories with political violence + future temporal: {df['story_count'].sum():,}")
print(f"Days with signal: {len(df)}")
print(f"Average per day (when present): {df['story_count'].mean():.1f}")

# Check if signal increases over time
q1 = df[df['date'] < '2024-04-01']['story_count'].sum()
q2 = df[(df['date'] >= '2024-04-01') & (df['date'] < '2024-07-01')]['story_count'].sum()
q3 = df[(df['date'] >= '2024-07-01') & (df['date'] < '2024-10-01')]['story_count'].sum()
q4 = df[df['date'] >= '2024-10-01']['story_count'].sum()

print(f"\nQuarterly breakdown:")
print(f"  Q1 (Jan-Mar): {q1:4d} stories")
print(f"  Q2 (Apr-Jun): {q2:4d} stories")
print(f"  Q3 (Jul-Sep): {q3:4d} stories")
print(f"  Q4 (Oct-Dec): {q4:4d} stories")

if q4 > q1:
    pct_increase = ((q4 - q1) / q1 * 100)
    print(f"\n✓ Signal INCREASED by {pct_increase:.0f}% from Q1 to Q4")
else:
    print(f"\n  Signal did not show consistent increase")

print("\nOpen with: xdg-open visualizations/anticipatory_signals_2025.png")