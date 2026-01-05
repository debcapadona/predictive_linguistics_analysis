"""
Interactive word burst explorer - PACKED BUBBLE LAYOUT
Each month shows top bursting words as packed circles
"""
import psycopg2
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np

print("="*70)
print("INTERACTIVE WORD BURST EXPLORER (PACKED)")
print("="*70)

pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)

def get_word_bursts(start_date, end_date, top_n_per_month=8):
    """Get top bursting words per month"""
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
                ob.avg_monthly_count as baseline,
                (mw.count - ob.avg_monthly_count) / NULLIF(ob.avg_monthly_count, 0) as burst_score,
                ROW_NUMBER() OVER (PARTITION BY mw.month ORDER BY (mw.count - ob.avg_monthly_count) / NULLIF(ob.avg_monthly_count, 0) DESC) as rank
            FROM monthly_words mw
            JOIN overall_baseline ob ON mw.word = ob.word
            WHERE mw.count > 20
            AND (mw.count - ob.avg_monthly_count) / NULLIF(ob.avg_monthly_count, 0) > 1.0
        )
        SELECT month, word, count, baseline, burst_score
        FROM ranked
        WHERE rank <= %s
        ORDER BY month, burst_score DESC
    """
    
    df = pd.read_sql(query, pg_conn, params=(start_date, end_date, start_date, end_date, top_n_per_month))
    return df

# Define timeframes
timeframes = {
    '2-Year': ('2024-01-01', '2025-12-05', 8),
    '2024': ('2024-01-01', '2024-12-31', 10),
    '2025': ('2025-01-01', '2025-12-05', 10),
    'Last 6mo': ((datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d'), '2025-12-05', 12),
    'Last 3mo': ((datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'), '2025-12-05', 15)
}

print("\nCreating packed bubble layouts...")

all_traces = {}

for name, (start, end, top_n) in timeframes.items():
    print(f"  Processing {name}...")
    df = get_word_bursts(start, end, top_n_per_month=top_n)
    
    if len(df) == 0:
        continue
    
    # Assign x-positions (months) and y-positions (packed within month)
    months = sorted(df['month'].unique())
    month_positions = {m: i for i, m in enumerate(months)}
    
    x_coords = []
    y_coords = []
    
    for month in months:
        month_data = df[df['month'] == month]
        n_words = len(month_data)
        
        # Simple packing: arrange in rows
        cols = int(np.ceil(np.sqrt(n_words)))
        
        for idx in range(n_words):
            row = idx // cols
            col = idx % cols
            
            # Add some jitter for visual appeal
            x_coords.append(month_positions[month] + (col - cols/2) * 0.15)
            y_coords.append(row * 1.5 + np.random.uniform(-0.1, 0.1))
    
    # Create hover text
    hover_text = [
        f"<b>{row['word']}</b><br>" +
        f"Month: {row['month'].strftime('%Y-%m')}<br>" +
        f"Burst Score: {row['burst_score']:.1f}x<br>" +
        f"Count: {row['count']}<br>" +
        f"Baseline: {row['baseline']:.1f}"
        for _, row in df.iterrows()
    ]
    
    trace = go.Scatter(
        x=x_coords,
        y=y_coords,
        mode='markers+text',
        name=name,
        text=df['word'],
        textposition='middle center',
        textfont=dict(size=8, color='white', family='Arial Black'),
        marker=dict(
            size=df['burst_score'] * 8 + 30,
            color=df['burst_score'],
            colorscale='Viridis',  # Purple-yellow-green
            showscale=True,
            colorbar=dict(title="Burst<br>Score"),
            line=dict(width=1, color='rgba(0,0,0,0.3)'),
            opacity=0.85
        ),
        hovertemplate='%{hovertext}<extra></extra>',
        hovertext=hover_text,
        visible=(name == '2-Year')
    )
    
    all_traces[name] = (trace, months)

# Create figure
fig = go.Figure()

# Add all traces
for name, (trace, months) in all_traces.items():
    fig.add_trace(trace)

# Create buttons
buttons = []
for idx, (name, (trace, months)) in enumerate(all_traces.items()):
    visibility = [False] * len(all_traces)
    visibility[idx] = True
    
    buttons.append(dict(
        label=name,
        method='update',
        args=[{'visible': visibility},
              {'title': f'Top Bursting Words by Month - {name}<br><sub>Bubble size = burst intensity | Hover for details</sub>',
               'xaxis': {'ticktext': [m.strftime('%b %Y') for m in months],
                        'tickvals': list(range(len(months)))}}]
    ))

# Get initial months for 2-Year
initial_months = all_traces['2-Year'][1]

# Layout
fig.update_layout(
    title='Top Bursting Words by Month - 2-Year<br><sub>Bubble size = burst intensity | Hover for details</sub>',
    xaxis=dict(
        title='Month',
        ticktext=[m.strftime('%b %Y') for m in initial_months],
        tickvals=list(range(len(initial_months))),
        showgrid=True,
        gridcolor='lightgray'
    ),
    yaxis=dict(
        title='',
        showticklabels=False,
        showgrid=False
    ),
    hovermode='closest',
    height=600,
    template='plotly_white',
    updatemenus=[dict(
        type='buttons',
        direction='left',
        buttons=buttons,
        pad={'r': 10, 't': 10},
        showactive=True,
        x=0.0,
        xanchor='left',
        y=1.12,
        yanchor='top',
        bgcolor='lightgray',
        active=0
    )],
    showlegend=False,
    plot_bgcolor='white'
)

# Save
output_file = 'visualizations/interactive_word_bursts.html'
fig.write_html(output_file)
print(f"\n✓ Saved: {output_file}")

pg_conn.close()

print("\n" + "="*70)
print("PACKED BUBBLE CHART COMPLETE")
print("="*70)
print(f"\nOpen with: xdg-open {output_file}")