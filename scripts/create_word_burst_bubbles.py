"""
Create bubble chart visualizations for word bursts
More engaging than heatmaps
"""
import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

print("="*70)
print("WORD BURST BUBBLE CHARTS")
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
        AND (mw.count - ob.avg_monthly_count) / NULLIF(ob.avg_monthly_count, 0) > 1.5
        ORDER BY mw.month, burst_score DESC
    """
    
    df = pd.read_sql(query, pg_conn, params=(start_date, end_date, start_date, end_date))
    return df

def create_bubble_chart(df, title, filename, top_n=30):
    """Create bubble chart of word bursts"""
    
    # Get top N words overall
    top_words = (
        df.groupby('word')['burst_score']
        .max()
        .nlargest(top_n)
        .index
    )
    
    df_filtered = df[df['word'].isin(top_words)]
    
    # Create figure
    fig, ax = plt.subplots(figsize=(20, 12))
    
    # Get unique months and words
    months = sorted(df_filtered['month'].unique())
    words = sorted(df_filtered['word'].unique())
    
    # Color map
    colors = plt.cm.rainbow(np.linspace(0, 1, len(words)))
    word_colors = dict(zip(words, colors))
    
    # Plot bubbles
    for _, row in df_filtered.iterrows():
        month_idx = months.index(row['month'])
        word_idx = words.index(row['word'])
        
        # Bubble size based on burst score
        size = (row['burst_score'] * 200) + 100
        
        ax.scatter(
            month_idx,
            word_idx,
            s=size,
            alpha=0.6,
            color=word_colors[row['word']],
            edgecolors='black',
            linewidth=1
        )
    
    # Format axes
    ax.set_xticks(range(len(months)))
    ax.set_xticklabels([m.strftime('%Y-%m') for m in months], rotation=45, ha='right')
    ax.set_yticks(range(len(words)))
    ax.set_yticklabels(words)
    
    ax.set_xlabel('Month', fontsize=14, fontweight='bold')
    ax.set_ylabel('Word', fontsize=14, fontweight='bold')
    ax.set_title(title, fontsize=18, fontweight='bold', pad=20)
    
    ax.grid(True, alpha=0.3, linestyle='--')
    
    # Add size legend
    legend_sizes = [2, 5, 10]
    legend_bubbles = [plt.scatter([], [], s=(s*200)+100, alpha=0.6, color='gray', edgecolors='black') 
                     for s in legend_sizes]
    ax.legend(legend_bubbles, [f'{s}x burst' for s in legend_sizes], 
             loc='upper left', title='Burst Score', framealpha=0.9)
    
    plt.tight_layout()
    plt.savefig(f'visualizations/{filename}', dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: visualizations/{filename}")

# Create visualizations
print("\nCreating bubble charts...")

# 2-year view
print("\n1. Two-year view (2024-2025)...")
df_2year = calculate_word_bursts('2024-01-01', '2025-12-05')
if len(df_2year) > 0:
    create_bubble_chart(df_2year, 'Word Burst Bubbles - 2024-2025', 'word_bursts_2year_bubbles.png', top_n=40)

# 2024 only
print("\n2. 2024 only...")
df_2024 = calculate_word_bursts('2024-01-01', '2024-12-31')
if len(df_2024) > 0:
    create_bubble_chart(df_2024, 'Word Burst Bubbles - 2024', 'word_bursts_2024_bubbles.png', top_n=30)

# Last 6 months
print("\n3. Last 6 months...")
six_mo = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
df_6mo = calculate_word_bursts(six_mo, '2025-12-05')
if len(df_6mo) > 0:
    create_bubble_chart(df_6mo, 'Word Burst Bubbles - Last 6 Months', 'word_bursts_6mo_bubbles.png', top_n=25)

pg_conn.close()

print("\n" + "="*70)
print("BUBBLE CHARTS COMPLETE")
print("="*70)
print("\nOpen with:")
print("  xdg-open visualizations/word_bursts_2year_bubbles.png")