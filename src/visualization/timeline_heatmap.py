"""
timeline_heatmap.py - Interactive timeline heatmap visualization

Shows word frequency z-scores over time as a heatmap
Helps identify which words "heat up" before events
"""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.core.database import Database
from src.core.stats_calculator import StatsCalculator


class TimelineHeatmap:
    """Generate timeline heatmap visualizations"""
    
    def __init__(self, db_path: str = "data/linguistic_predictor.db"):
        """
        Initialize visualizer
        
        Args:
            db_path: Path to database
        """
        self.db = Database(db_path)
    
    def get_word_frequencies_by_week(self, start_date: str, end_date: str, 
                                     top_n_words: int = 20) -> pd.DataFrame:
        """
        Get word frequencies grouped by week
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            top_n_words: Number of top words to include
            
        Returns:
            DataFrame with columns: word, week_start, frequency, z_score
        """
        print(f"Analyzing period: {start_date} to {end_date}")
        
        # Get all stories in range
        stories = self.db.get_stories_by_date_range(start_date, end_date)
        print(f"Found {len(stories)} stories")
        
        if not stories:
            print("No stories found in date range!")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(stories)
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        # Group by week
        df['week_start'] = df['created_at'].dt.to_period('W').dt.start_time
        
        # Get all words across all stories
        all_words = []
        for _, row in df.iterrows():
            words = row['words'].split('|') if row['words'] else []
            all_words.extend(words)
        
        # Find top N most frequent words overall
        from collections import Counter
        word_counts = Counter(all_words)
        top_words = [word for word, _ in word_counts.most_common(top_n_words)]
        
        print(f"Tracking top {len(top_words)} words")
        
        # Calculate frequency per week for each top word
        results = []
        
        weeks = sorted(df['week_start'].unique())
        print(f"Analyzing {len(weeks)} weeks...")
        
        for word in top_words:
            weekly_counts = []
            
            for week in weeks:
                week_stories = df[df['week_start'] == week]
                
                # Count word occurrences this week
                count = 0
                for _, row in week_stories.iterrows():
                    words = row['words'].split('|') if row['words'] else []
                    count += words.count(word)
                
                weekly_counts.append(count)
            
            # Calculate z-scores (baseline = first 2 weeks)
            if len(weekly_counts) >= 3:
                baseline_counts = weekly_counts[:2]
                baseline_mean = np.mean(baseline_counts)
                baseline_std = np.std(baseline_counts) if len(baseline_counts) > 1 else baseline_mean * 0.3
                
                if baseline_std == 0:
                    baseline_std = 1.0
                
                for i, (week, count) in enumerate(zip(weeks, weekly_counts)):
                    z_score = (count - baseline_mean) / baseline_std if baseline_std > 0 else 0
                    
                    results.append({
                        'word': word,
                        'week_start': week,
                        'frequency': count,
                        'z_score': z_score
                    })
        
        result_df = pd.DataFrame(results)
        print(f"Generated {len(result_df)} data points")
        
        return result_df
    
    def create_heatmap(self, df: pd.DataFrame, title: str = "Linguistic Signal Timeline",
                      output_file: str = "timeline_heatmap.html"):
        """
        Create interactive heatmap
        
        Args:
            df: DataFrame with word, week_start, frequency, z_score
            title: Chart title
            output_file: Output HTML file
        """
        if df.empty:
            print("No data to visualize!")
            return
        
        print(f"\nCreating heatmap...")
        
        # Pivot data for heatmap
        heatmap_data = df.pivot(index='word', columns='week_start', values='z_score')
        
        # Sort words by maximum z-score (most interesting at top)
        word_max_z = heatmap_data.max(axis=1).sort_values(ascending=False)
        heatmap_data = heatmap_data.loc[word_max_z.index]
        
        # Format week labels
        week_labels = [date.strftime('%b %d') for date in heatmap_data.columns]
        
        # Create hover text with frequency info
        hover_data = df.pivot(index='word', columns='week_start', values='frequency')
        hover_data = hover_data.loc[heatmap_data.index]
        
        hover_text = []
        for i, word in enumerate(heatmap_data.index):
            row_hover = []
            for j, week in enumerate(heatmap_data.columns):
                z = heatmap_data.iloc[i, j]
                freq = hover_data.iloc[i, j]
                text = f"Word: {word}<br>Week: {week.strftime('%Y-%m-%d')}<br>Frequency: {freq}<br>Z-Score: {z:.2f}"
                row_hover.append(text)
            hover_text.append(row_hover)
        
        # Create figure
        fig = go.Figure(data=go.Heatmap(
            z=heatmap_data.values,
            x=week_labels,
            y=heatmap_data.index,
            colorscale='RdYlBu_r',  # Red for high, blue for low
            zmid=0,  # Center at 0
            text=hover_text,
            hovertemplate='%{text}<extra></extra>',
            colorbar=dict(
                title=dict(text="Z-Score", side="right"), 
                tickmode="linear",
                tick0=-2,
                dtick=1
            )
        ))
        
        # Update layout
        fig.update_layout(
            title={
                'text': title,
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 24}
            },
            xaxis_title="Week Starting",
            yaxis_title="Word",
            height=max(600, len(heatmap_data) * 30),
            width=1400,
            font=dict(size=12),
            xaxis=dict(tickangle=-45),
            margin=dict(l=150, r=100, t=100, b=100)
        )
        
        # Add annotations for significant events
        # You can customize these based on known events
        annotations = []
        
        # Example: Mark July 13 (Trump assassination attempt)
        for i, week in enumerate(heatmap_data.columns):
            if week.month == 7 and week.day >= 13 and week.day < 20:
                annotations.append(
                    dict(
                        x=i,
                        y=-0.5,
                        text="📍 July 13 Event",
                        showarrow=True,
                        arrowhead=2,
                        ax=0,
                        ay=-40,
                        font=dict(size=10, color='red')
                    )
                )
        
        if annotations:
            fig.update_layout(annotations=annotations)
        
        # Save to file
        output_path = f"data/visualizations/{output_file}"
        os.makedirs("data/visualizations", exist_ok=True)
        
        fig.write_html(output_path)
        print(f"✅ Heatmap saved to: {output_path}")
        
        # Also show in browser
        fig.show()
    
    def close(self):
        """Close database connection"""
        self.db.close()


# Main execution
if __name__ == "__main__":
    print("="*80)
    print("TIMELINE HEATMAP GENERATOR")
    print("="*80)
    
    viz = TimelineHeatmap()
    
    # Generate heatmap for May-July 2024
    print("\nGenerating May-July 2024 heatmap...")
    df = viz.get_word_frequencies_by_week(
        start_date="2024-05-01",
        end_date="2024-08-01",
        top_n_words=30  # Track top 30 words
    )
    
    if not df.empty:
        viz.create_heatmap(
            df,
            title="Linguistic Signals: May-July 2024 (Trump Assassination & NVDA $3T)",
            output_file="may_july_2024_heatmap.html"
        )
    
    viz.close()
    
    print("\n✅ Visualization complete!")
    print("Open the HTML file in your browser to view the interactive heatmap")