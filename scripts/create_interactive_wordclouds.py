"""
Interactive word clouds by month - using markers with text
"""
import psycopg2
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np

print("="*70)
print("INTERACTIVE WORD CLOUDS BY MONTH")
print("="*70)

pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)

def get_monthly_bursts(start_date, end_date, top_n=30):
    """Get bursting words grouped by month"""
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
        ),
        ranked AS (
            SELECT 
                mw.month,
                mw.word,
                mw.count,
                (mw.count - ob.avg_monthly_count) / NULLIF(ob.avg_monthly_count, 0) as burst_score,
                ROW_NUMBER() OVER (PARTITION BY mw.month ORDER BY (mw.count - ob.avg_monthly_count) / NULLIF(ob.avg_monthly_count, 0) DESC) as rank
            FROM monthly_words mw
            JOIN overall_baseline ob ON mw.word = ob.word
            WHERE mw.count > 20
            AND (mw.count - ob.avg_monthly_count) / NULLIF(ob.avg_monthly_count, 0) > 1.0
        )
        SELECT month, word, count, burst_score
        FROM ranked
        WHERE rank <= %s
        ORDER BY month, burst_score DESC
    """
    
    df = pd.read_sql(query, pg_conn, params=(start_date, end_date, start_date, end_date, top_n))
    return df

print("\nLoading word burst data...")
df = get_monthly_bursts('2024-01-01', '2025-12-05', top_n=35)

months = sorted(df['month'].unique())
print(f"  ✓ Found {len(months)} months with data")

# Create traces for each month
traces = []
month_names = []

for month in months:
    month_data = df[df['month'] == month].copy()
    
    # Spiral layout
    n_words = len(month_data)
    angles = np.linspace(0, 6*np.pi, n_words)
    radii = np.linspace(0.5, 3, n_words)
    
    x_coords = radii * np.cos(angles)
    y_coords = radii * np.sin(angles)
    
    # Size and color based on burst score
    sizes = month_data['burst_score'] * 20 + 30
    
    hover_text = [
        f"<b>{row['word']}</b><br>" +
        f"Burst: {row['burst_score']:.1f}x<br>" +
        f"Count: {row['count']}"
        for _, row in month_data.iterrows()
    ]
    
    trace = go.Scatter(
        x=x_coords,
        y=y_coords,
        mode='markers+text',
        text=month_data['word'],
        textposition='middle center',
        textfont=dict(size=11, color='white', family='Arial Black'),
        marker=dict(
            size=sizes,
            color=month_data['burst_score'],
            colorscale='Viridis',
            showscale=(month == months[0]),
            colorbar=dict(title="Burst<br>Score"),
            line=dict(width=1, color='rgba(0,0,0,0.2)')
        ),
        hovertext=hover_text,
        hovertemplate='%{hovertext}<extra></extra>',
        name=month.strftime('%B %Y'),
        visible=(month == months[0])
    )
    
    traces.append(trace)
    month_names.append(month.strftime('%B %Y'))

# Create figure
fig = go.Figure(data=traces)

# Create slider
steps = []
for i, month_name in enumerate(month_names):
    visibility = [False] * len(traces)
    visibility[i] = True
    
    step = dict(
        method="update",
        args=[{"visible": visibility}],
        label=month_name
    )
    steps.append(step)

sliders = [dict(
    active=0,
    currentvalue=dict(prefix="Month: ", visible=True, xanchor="center"),
    pad={"b": 10, "t": 50},
    len=0.9,
    x=0.05,
    steps=steps
)]

# Layout
fig.update_layout(
    title=f"Top Bursting Words - {month_names[0]}<br><sub>Use slider to navigate months | Hover for details</sub>",
    xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
    yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
    sliders=sliders,
    height=700,
    template='plotly_white',
    showlegend=False,
    hovermode='closest'
)

# Save
output_file = 'visualizations/interactive_wordcloud.html'
fig.write_html(output_file)
print(f"\n✓ Saved: {output_file}")

pg_conn.close()

print("\n" + "="*70)
print("INTERACTIVE WORD CLOUD COMPLETE")
print("="*70)
print(f"\nOpen with: xdg-open {output_file}")