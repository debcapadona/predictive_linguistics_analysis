"""
Create bubble chart with words embedded in bubbles
"""
import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

print("="*70)
print("WORD BURST BUBBLE CHARTS (Words in Bubbles)")
print("="*70)

pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)

def calculate_word_bursts(start_date, end_date):
    """Get word bursts for date range"""
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
        AND (mw.count - ob.avg_monthly_count) / NULLIF(ob.avg_monthly_count, 0) > 2.0
        ORDER BY mw.month, burst_score DESC
    """
    
    df = pd.read_sql(query, pg_conn, params=(start_date, end_date, start_date, end_date))
    return df

def create_bubble_chart_with_labels(df, title, filename, top_n_per_month=8):
    """Create bubble chart with word labels inside bubbles"""
    
    # Get top N words per month
    top_words_df = (
        df.groupby('month')
        .apply(lambda x: x.nlargest(top_n_per_month, 'burst_score'))
        .reset_index(drop=True)
    )
    
    if len(top_words_df) == 0:
        print("  ⚠️  No data to visualize")
        return
    
    # Create figure
    fig, ax = plt.subplots(figsize=(24, 14))
    
    # Get unique months
    months = sorted(top_words_df['month'].unique())
    month_positions = {m: i for i, m in enumerate(months)}
    
    # Assign y-positions to avoid overlap
    month_word_y = {}
    for month in months:
        month_data = top_words_df[top_words_df['month'] == month].nlargest(top_n_per_month, 'burst_score')
        for idx, (_, row) in enumerate(month_data.iterrows()):
            month_word_y[(month, row['word'])] = idx
    
    # Plot bubbles
    for _, row in top_words_df.iterrows():
        x = month_positions[row['month']]
        y = month_word_y.get((row['month'], row['word']), 0)
        
        # Bubble size based on burst score
        size = min((row['burst_score'] * 1000) + 500, 5000)
        
        # Color based on burst intensity
        color_intensity = min(row['burst_score'] / 10, 1.0)
        color = plt.cm.YlOrRd(color_intensity)
        
        # Draw bubble
        circle = plt.Circle((x, y), radius=0.4, 
                          color=color, alpha=0.7,
                          edgecolor='black', linewidth=2)
        ax.add_patch(circle)
        
        # Add word label
        ax.text(x, y, row['word'], 
               ha='center', va='center',
               fontsize=10, fontweight='bold',
               color='white' if color_intensity > 0.5 else 'black')
        
        # Add burst score as small text below word
        ax.text(x, y - 0.15, f"{row['burst_score']:.1f}x",
               ha='center', va='center',
               fontsize=7, style='italic',
               color='white' if color_intensity > 0.5 else 'black')
    
    # Format axes
    ax.set_xlim(-1, len(months))
    ax.set_ylim(-1, top_n_per_month)
    
    ax.set_xticks(range(len(months)))
    ax.set_xticklabels([m.strftime('%Y-%m') for m in months], 
                       rotation=45, ha='right', fontsize=11)
    ax.set_yticks([])
    
    ax.set_xlabel('Month', fontsize=14, fontweight='bold')
    ax.set_title(title, fontsize=18, fontweight='bold', pad=20)
    
    ax.spines['left'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    
    ax.grid(True, alpha=0.2, axis='x', linestyle='--')
    
    plt.tight_layout()
    plt.savefig(f'visualizations/{filename}', dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: visualizations/{filename}")

# Create visualizations
print("\nCreating bubble charts with embedded words...")

# 2-year view
print("\n1. Two-year view (2024-2025)...")
df_2year = calculate_word_bursts('2024-01-01', '2025-12-05')
if len(df_2year) > 0:
    create_bubble_chart_with_labels(df_2year, 
                                   'Top Bursting Words by Month - 2024-2025', 
                                   'word_bursts_2year_labeled.png', 
                                   top_n_per_month=10)

# 2024 only
print("\n2. 2024 only...")
df_2024 = calculate_word_bursts('2024-01-01', '2024-12-31')
if len(df_2024) > 0:
    create_bubble_chart_with_labels(df_2024, 
                                   'Top Bursting Words by Month - 2024', 
                                   'word_bursts_2024_labeled.png',
                                   top_n_per_month=8)

# Last 6 months
print("\n3. Last 6 months...")
six_mo = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
df_6mo = calculate_word_bursts(six_mo, '2025-12-05')
if len(df_6mo) > 0:
    create_bubble_chart_with_labels(df_6mo, 
                                   'Top Bursting Words - Last 6 Months', 
                                   'word_bursts_6mo_labeled.png',
                                   top_n_per_month=8)

pg_conn.close()

print("\n" + "="*70)
print("LABELED BUBBLE CHARTS COMPLETE")
print("="*70)