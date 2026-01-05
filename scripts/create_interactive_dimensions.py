"""
Create interactive Plotly timeline of all 9 dimensions
Features:
- Hover to see story titles from that day
- Toggle dimensions on/off
- Zoom into events
- Annotations for known events
"""
import psycopg2
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime

print("="*70)
print("INTERACTIVE DIMENSION TIMELINE (PLOTLY)")
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

# Load all dimension data with sample story titles
all_data = {}
for dim in dimensions:
    query = f"""
        SELECT 
            DATE(s.created_at) as date,
            AVG(b.score) as avg_score,
            COUNT(*) as story_count,
            STRING_AGG(DISTINCT s.title, ' | ') as sample_titles
        FROM bert_{dim} b
        JOIN stories s ON b.story_id = s.id
        WHERE DATE(s.created_at) BETWEEN '2024-01-01' AND '2025-12-05'
        GROUP BY DATE(s.created_at)
        ORDER BY DATE(s.created_at)
    """
    df = pd.read_sql(query, pg_conn)
    df['date'] = pd.to_datetime(df['date'])
    df['smoothed'] = df['avg_score'].rolling(window=7, center=True).mean()
    
    # Limit title length for hover
    df['sample_titles'] = df['sample_titles'].str[:200] + '...'
    
    all_data[dim] = df

print(f"  ✓ Loaded {len(dimensions)} dimensions")

# Create figure
fig = go.Figure()

# Color palette
colors = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22'
]

# Add traces for each dimension
for idx, dim in enumerate(dimensions):
    df = all_data[dim]
    
    # Create hover text with story titles
    hover_text = [
        f"<b>{dim.replace('_', ' ').title()}</b><br>" +
        f"Date: {date.strftime('%Y-%m-%d')}<br>" +
        f"Score: {score:.4f}<br>" +
        f"Stories: {count}<br>" +
        f"<br><i>Sample titles:</i><br>{titles[:150]}..."
        for date, score, count, titles in zip(
            df['date'], df['smoothed'], df['story_count'], df['sample_titles']
        )
    ]
    
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['smoothed'],
        mode='lines',
        name=dim.replace('_', ' ').title(),
        line=dict(color=colors[idx], width=2),
        hovertemplate='%{text}<extra></extra>',
        text=hover_text,
        visible=True
    ))

# Add event annotations
events = [
    {'date': '2024-06-12', 'name': 'Reddit API Blackout', 'color': 'red'},
    {'date': '2024-07-13', 'name': 'Trump Assassination Attempt', 'color': 'orange'},
    {'date': '2024-11-05', 'name': 'Election Day', 'color': 'blue'},
    {'date': '2025-06-15', 'name': 'MN Lawmakers Shooting', 'color': 'purple'},
    {'date': '2025-09-10', 'name': 'Charlie Kirk Assassination', 'color': 'darkred'},
]

for event in events:
    fig.add_vline(
        x=event['date'],
        line_dash="dash",
        line_color=event['color'],
        line_width=2,
        annotation_text=event['name'],
        annotation_position="top"
    )

# Update layout
fig.update_layout(
    title={
        'text': 'Interactive Linguistic Dimensions Timeline - Hacker News 2024-2025',
        'font': {'size': 20, 'family': 'Arial Black'}
    },
    xaxis_title='Date',
    yaxis_title='Dimension Score (7-day moving average)',
    hovermode='closest',
    height=800,
    template='plotly_white',
    legend=dict(
        orientation="v",
        yanchor="top",
        y=1,
        xanchor="left",
        x=1.02
    ),
    font=dict(size=12)
)

# Add range slider
fig.update_xaxes(
    rangeslider_visible=True,
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=3, label="3m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="1y", step="year", stepmode="backward"),
            dict(step="all", label="All")
        ])
    )
)

# Save as HTML
output_file = 'visualizations/interactive_dimensions.html'
fig.write_html(output_file)
print(f"\n✓ Saved: {output_file}")

# Also create Event Coherence overlay version
print("\nCreating version with Event Coherence Index...")

# Load ECI data
eci_df = pd.read_csv('data/event_coherence_index.csv')
eci_df['date'] = pd.to_datetime(eci_df['date'])

fig2 = go.Figure()

# Add all dimension traces (lighter/thinner)
for idx, dim in enumerate(dimensions):
    df = all_data[dim]
    fig2.add_trace(go.Scatter(
        x=df['date'],
        y=df['smoothed'],
        mode='lines',
        name=dim.replace('_', ' ').title(),
        line=dict(color=colors[idx], width=1),
        opacity=0.5,
        showlegend=True
    ))

# Add ECI trace (prominent)
fig2.add_trace(go.Scatter(
    x=eci_df['date'],
    y=eci_df['event_coherence_index'],
    mode='lines',
    name='Event Coherence Index',
    line=dict(color='red', width=4),
    yaxis='y2',
    hovertemplate='<b>Event Coherence</b><br>Date: %{x}<br>Score: %{y:.3f}<extra></extra>'
))

# Add threshold line
fig2.add_hline(
    y=0.5,
    line_dash="dash",
    line_color="orange",
    line_width=2,
    annotation_text="High Coherence Threshold",
    yref='y2'
)

# Add events
for event in events:
    fig2.add_vline(
        x=event['date'],
        line_dash="dash",
        line_color=event['color'],
        line_width=2,
        annotation_text=event['name'],
        annotation_position="top"
    )

# Update layout with dual y-axis
fig2.update_layout(
    title='Linguistic Dimensions + Event Coherence Index',
    xaxis_title='Date',
    yaxis_title='Dimension Score',
    yaxis2=dict(
        title='Event Coherence Index',
        overlaying='y',
        side='right',
        range=[0, 1]
    ),
    hovermode='closest',
    height=800,
    template='plotly_white'
)

fig2.update_xaxes(rangeslider_visible=True)

output_file2 = 'visualizations/interactive_dimensions_with_eci.html'
fig2.write_html(output_file2)
print(f"✓ Saved: {output_file2}")

pg_conn.close()

print("\n" + "="*70)
print("INTERACTIVE VISUALIZATIONS COMPLETE")
print("="*70)
print("\nOpen in browser:")
print(f"  xdg-open {output_file}")
print(f"  xdg-open {output_file2}")