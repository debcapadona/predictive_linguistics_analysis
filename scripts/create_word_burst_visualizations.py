"""
Create word burst visualizations for multiple timeframes
Shows top bursting words by month
"""
import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

print("="*70)
print("WORD BURST VISUALIZATIONS")
print("="*70)

pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)

def calculate_word_bursts(start_date, end_date, label):
    """
    Calculate word bursts for a date range
    Returns top bursting words by month
    """
    print(f"\n{label}: {start_date} to {end_date}")
    
    # Get word frequencies by month
    query = """
        WITH monthly_words AS (
            SELECT 
                DATE_TRUNC('month', s.created_at) as month,
                LOWER(wt.word_text) as word,
                COUNT(*) as count
            FROM word_tokens wt
            JOIN stories s ON wt.story_id = s.id
            WHERE DATE(s.created_at) BETWEEN %s AND %s
            AND LENGTH(wt.word_text) > 3
            GROUP BY month, word
        ),
        overall_baseline AS (
            SELECT 
                LOWER(wt.word_text) as word,
                COUNT(*) / COUNT(DISTINCT DATE_TRUNC('month', s.created_at)) as avg_monthly_count
            FROM word_tokens wt
            JOIN stories s ON wt.story_id = s.id
            WHERE DATE(s.created_at) BETWEEN %s AND %s
            AND LENGTH(wt.word_text) > 3
            GROUP BY word
            HAVING COUNT(*) > 50
        )
        SELECT 
            mw.month,
            mw.word,
            mw.count,
            ob.avg_monthly_count as baseline,
            (mw.count - ob.avg_monthly_count) / NULLIF(ob.avg_monthly_count, 0) as burst_score
        FROM monthly_words mw
        JOIN overall_baseline ob ON mw.word = ob.word
        WHERE mw.count > 20
        AND (mw.count - ob.avg_monthly_count) / NULLIF(ob.avg_monthly_count, 0) > 1.5
        ORDER BY mw.month, burst_score DESC
    """
    
    df = pd.read_sql(query, pg_conn, params=(start_date, end_date, start_date, end_date))
    
    if len(df) == 0:
        print(f"  ⚠️  No burst data found")
        return None
    
    print(f"  ✓ Found {len(df)} bursting words across {df['month'].nunique()} months")
    return df

def visualize_bursts(df, title, filename, top_n=15):
    """Create heatmap visualization of top bursting words by month"""
    if df is None or len(df) == 0:
        return
    
    # Get top N words per month
    top_words_per_month = (
        df.groupby('month')
        .apply(lambda x: x.nlargest(top_n, 'burst_score'))
        .reset_index(drop=True)
    )
    
    # Pivot for heatmap
    pivot = top_words_per_month.pivot_table(
        index='word',
        columns='month',
        values='burst_score',
        fill_value=0
    )
    
    # Get top words overall
    word_totals = pivot.sum(axis=1).sort_values(ascending=False)
    top_words = word_totals.head(50).index
    pivot_filtered = pivot.loc[top_words]
    
    # Create figure
    fig, ax = plt.subplots(figsize=(16, 12))
    
    sns.heatmap(
        pivot_filtered,
        cmap='YlOrRd',
        annot=False,
        cbar_kws={'label': 'Burst Score'},
        linewidths=0.5,
        ax=ax
    )
    
    ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Month', fontsize=12, fontweight='bold')
    ax.set_ylabel('Word', fontsize=12, fontweight='bold')
    
    # Format month labels
    month_labels = [col.strftime('%Y-%m') for col in pivot_filtered.columns]
    ax.set_xticklabels(month_labels, rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(f'visualizations/{filename}', dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: visualizations/{filename}")
    
    # Also create top 10 list per month
    print(f"\n  Top 5 bursting words by month:")
    for month in sorted(df['month'].unique()):
        month_data = df[df['month'] == month].nlargest(5, 'burst_score')
        words = ', '.join(month_data['word'].tolist())
        print(f"    {month.strftime('%Y-%m')}: {words}")

# Calculate for different timeframes
print("\nCalculating word bursts for different timeframes...")

# 1. Two-year view (2024-2025)
df_2year = calculate_word_bursts('2024-01-01', '2025-12-05', '2-Year View (2024-2025)')
if df_2year is not None:
    visualize_bursts(df_2year, 'Word Bursts - 2024-2025 (2 Years)', 'word_bursts_2year.png', top_n=20)

# 2. 2024 only
df_2024 = calculate_word_bursts('2024-01-01', '2024-12-31', '2024 Full Year')
if df_2024 is not None:
    visualize_bursts(df_2024, 'Word Bursts - 2024', 'word_bursts_2024.png')

# 3. 2025 only (so far)
df_2025 = calculate_word_bursts('2025-01-01', '2025-12-05', '2025 Year-to-Date')
if df_2025 is not None:
    visualize_bursts(df_2025, 'Word Bursts - 2025 (Jan-Dec)', 'word_bursts_2025.png')

# 4. Last 6 months
six_mo_ago = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
df_6mo = calculate_word_bursts(six_mo_ago, '2025-12-05', 'Last 6 Months')
if df_6mo is not None:
    visualize_bursts(df_6mo, 'Word Bursts - Last 6 Months', 'word_bursts_6mo.png')

# 5. Last 3 months
three_mo_ago = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
df_3mo = calculate_word_bursts(three_mo_ago, '2025-12-05', 'Last 3 Months')
if df_3mo is not None:
    visualize_bursts(df_3mo, 'Word Bursts - Last 3 Months', 'word_bursts_3mo.png')

# Save data
print("\nSaving burst data...")
if df_2year is not None:
    df_2year.to_csv('data/word_bursts_2year.csv', index=False)
if df_2024 is not None:
    df_2024.to_csv('data/word_bursts_2024.csv', index=False)
if df_2025 is not None:
    df_2025.to_csv('data/word_bursts_2025.csv', index=False)

pg_conn.close()

print("\n" + "="*70)
print("WORD BURST VISUALIZATIONS COMPLETE")
print("="*70)
print("\nCreated visualizations:")
print("  - visualizations/word_bursts_2year.png (2024-2025)")
print("  - visualizations/word_bursts_2024.png")
print("  - visualizations/word_bursts_2025.png")
print("  - visualizations/word_bursts_6mo.png")
print("  - visualizations/word_bursts_3mo.png")