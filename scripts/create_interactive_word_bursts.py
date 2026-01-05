"""
Interactive word burst explorer with timeframe buttons
"""
import psycopg2
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta

print("="*70)
print("INTERACTIVE WORD BURST EXPLORER")
print("="*70)

pg_conn = psycopg2.connect(
    host='localhost', port=5432,
    database='linguistic_predictor_v2',
    user='analyzer', password='dev_password_change_in_prod'
)

def get_word_bursts(start_date, end_date, top_n=40):
    """Get top bursting words for date range"""
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
        AND (mw.count - ob.avg_monthly_count) / NULLIF(ob.avg_monthly_count, 0) > 1.0
        ORDER BY burst_score DESC
        LIMIT %s
    """
    
    df = pd.read_sql(query, pg_conn, params=(start_date, end_date, start_date, end_date, top_n))
    return df

# Define timeframes
timeframes = {
    '2-Year': ('2024-01-01', '2025-12-05'),
    '2024': ('2024-01-01', '2024-12-31'),
    '2025': ('2025-01-01', '2025-12-05'),
    'Last 6mo': ((datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d'), '2025-12-05'),
    'Last 3mo': ((datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'), '2025-12-05')
}

print("\nLoading word bursts for all timeframes...")

all_traces = {}

for name, (start, end) in timeframes.items():
    print(f"  Loading {name}...")
    df = get_word_bursts(start, end, top_n=50)
    
    if len(df) == 0:
        continue
    
    # Create bubble trace
    hover_text = [
        f"<b>{row['word']}</b><br>" +
        f"Month: {row['month'].strftime('%Y-%m')}<br>" +
        f"Burst Score: {row['burst_score']:.1f}x<br>" +
        f"Count: {row['count']}<br>" +
        f"Baseline: {row['baseline']:.1f}"
        for _, row in df.iterrows()
    ]
    
    trace = go.Scatter(
        x=df['month'],
        y=df['burst_score'],
        mode='markers+text',
        name=name,
        text=df['word'],
        textposition='middle center',
        textfont=dict(size=9, color='white'),
        marker=dict(
            size=df['burst_score'] * 5 + 20,
            color=df['burst_score'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="Burst<br>Score"),
            line=dict(width=2, color='black')
        ),
        hovertemplate='%{text}<extra></extra>',
        hovertext=hover_text,
        visible=(name == '2-Year')  # Show 2-Year by default
    )
    
    all_traces[name] = trace

# Create figure
fig = go.Figure()

# Add all traces
for name, trace in all_traces.items():
    fig.add_trace(trace)

# Create buttons for timeframe selection
buttons = []
for idx, name in enumerate(all_traces.keys()):
    visibility = [False] * len(all_traces)
    visibility[idx] = True
    
    buttons.append(dict(
        label=name,
        method='update',
        args=[{'visible': visibility},
              {'title': f'Word Bursts - {name}<br><sub>Bubble size = burst intensity | Hover for details</sub>'}]
    ))

# Layout
fig.update_layout(
    title='Word Bursts - 2-Year<br><sub>Bubble size = burst intensity | Hover for details</sub>',
    xaxis_title='Month',
    yaxis_title='Burst Score (multiples of baseline)',
    hovermode='closest',
    height=700,
    template='plotly_white',
    updatemenus=[dict(
        type='buttons',
        direction='left',
        buttons=buttons,
        pad={'r': 10, 't': 10},
        showactive=True,
        x=0.0,
        xanchor='left',
        y=1.15,
        yanchor='top',
        bgcolor='lightgray',
        active=0
    )],
    showlegend=False
)

# Save
output_file = 'visualizations/interactive_word_bursts.html'
fig.write_html(output_file)
print(f"\n✓ Saved: {output_file}")

pg_conn.close()

print("\n" + "="*70)
print("INTERACTIVE WORD BURST EXPLORER COMPLETE")
print("="*70)
print(f"\nOpen with: xdg-open {output_file}")