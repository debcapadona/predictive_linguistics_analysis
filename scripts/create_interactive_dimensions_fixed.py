"""
Interactive Plotly dimension timeline - FIXED
Hover to see stories, toggle dimensions, zoom, explore
"""
import psycopg2
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

print("="*70)
print("INTERACTIVE DIMENSION TIMELINE")
print("="*70)

pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)

dimensions = [
    'emotional_valence_shift', 'temporal_bleed', 'certainty_collapse',
    'time_compression', 'agency_reversal', 'metaphor_cluster_density',
    'novel_meme_explosion', 'sacred_profane_ratio', 'pronoun_flip'
]

print("\nLoading dimension data...")

all_data = {}
for dim in dimensions:
    query = f"""
        SELECT 
            DATE(s.created_at) as date,
            AVG(b.score) as avg_score,
            COUNT(*) as story_count,
            STRING_AGG(DISTINCT SUBSTRING(s.title, 1, 80), ' | ') as sample_titles
        FROM bert_{dim} b
        JOIN stories s ON b.story_id = s.id
        WHERE DATE(s.created_at) BETWEEN '2024-01-01' AND '2025-12-05'
        GROUP BY DATE(s.created_at)
        ORDER BY DATE(s.created_at)
    """
    df = pd.read_sql(query, pg_conn)
    df['date'] = pd.to_datetime(df['date'])
    df['smoothed'] = df['avg_score'].rolling(window=7, center=True).mean()
    all_data[dim] = df

print(f"  ✓ Loaded {len(dimensions)} dimensions")

# Create figure
fig = go.Figure()

colors = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22'
]

# Add dimension traces
for idx, dim in enumerate(dimensions):
    df = all_data[dim]
    
    hover_text = [
        f"<b>{dim.replace('_', ' ').title()}</b><br>" +
        f"Date: {date.strftime('%Y-%m-%d')}<br>" +
        f"Score: {score:.4f}<br>" +
        f"Stories: {count}"
        for date, score, count in zip(df['date'], df['smoothed'], df['story_count'])
    ]
    
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['smoothed'],
        mode='lines',
        name=dim.replace('_', ' ').title(),
        line=dict(color=colors[idx], width=2),
        hovertemplate='%{text}<extra></extra>',
        text=hover_text
    ))

# Add event markers using shapes (not add_vline which has a bug)
events = [
    {'date': '2024-06-12', 'name': 'Reddit API Blackout', 'color': 'red'},
    {'date': '2024-07-13', 'name': 'Trump Assassination Attempt', 'color': 'orange'},
    {'date': '2024-11-05', 'name': 'Election Day', 'color': 'blue'},
    {'date': '2025-06-15', 'name': 'MN Lawmakers Shooting', 'color': 'purple'},
    {'date': '2025-09-10', 'name': 'Charlie Kirk Assassination', 'color': 'darkred'},
]

shapes = []
annotations = []

for event in events:
    # Vertical line shape
    shapes.append(dict(
        type='line',
        x0=event['date'],
        x1=event['date'],
        y0=0,
        y1=1,
        yref='paper',
        line=dict(color=event['color'], width=2, dash='dash')
    ))
    
    # Label annotation
    annotations.append(dict(
        x=event['date'],
        y=1,
        yref='paper',
        text=event['name'],
        showarrow=False,
        yanchor='bottom',
        font=dict(size=10, color=event['color']),
        bgcolor='rgba(255,255,255,0.8)'
    ))

# Layout
fig.update_layout(
    title={
        'text': 'Linguistic Dimensions Timeline - Hacker News 2024-2025<br><sub>Hover over lines to see details | Toggle dimensions in legend | Use slider to zoom</sub>',
        'font': {'size': 18, 'family': 'Arial'}
    },
    xaxis_title='Date',
    yaxis_title='Dimension Score (7-day moving average)',
    hovermode='closest',
    height=700,
    template='plotly_white',
    shapes=shapes,
    annotations=annotations,
    legend=dict(
        orientation="v",
        yanchor="top",
        y=1,
        xanchor="left",
        x=1.01,
        font=dict(size=10)
    )
)

# Add range slider and selectors
fig.update_xaxes(
    rangeslider_visible=True,
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1M", step="month", stepmode="backward"),
            dict(count=3, label="3M", step="month", stepmode="backward"),
            dict(count=6, label="6M", step="month", stepmode="backward"),
            dict(count=1, label="1Y", step="year", stepmode="backward"),
            dict(step="all", label="All")
        ]),
        bgcolor="lightgray",
        activecolor="darkgray"
    )
)

# Save
output_file = 'visualizations/interactive_dimensions.html'
fig.write_html(output_file)
print(f"\n✓ Saved: {output_file}")

pg_conn.close()

print("\n" + "="*70)
print("INTERACTIVE TIMELINE COMPLETE")
print("="*70)
print(f"\nOpen with: xdg-open {output_file}")