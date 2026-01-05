"""
emotional_valence.py - Dimension 3: Emotional Valence Shift

Measures emotional tone using VADER sentiment analysis
Score: -1.0 (negative) to +1.0 (positive)
"""

from typing import Dict


class EmotionalValenceAnalyzer:
    """Analyze emotional valence using sentiment"""
    
    def __init__(self):
        """Initialize VADER sentiment analyzer"""
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
            self.analyzer = SentimentIntensityAnalyzer()
            self.available = True
        except ImportError:
            print("⚠️  VADER not installed. Install with: pip install vaderSentiment")
            self.analyzer = None
            self.available = False
    
    def score(self, text: str) -> Dict:
        """
        Calculate emotional valence score for text
        
        Args:
            text: Raw text (title or comment)
            
        Returns:
            Dictionary with score and details
        """
        if not self.available or not text:
            return {
                'score': 0.0,
                'positive': 0.0,
                'negative': 0.0,
                'neutral': 0.0
            }
        
        # Get VADER scores
        scores = self.analyzer.polarity_scores(text)
        
        return {
            'score': round(scores['compound'], 3),  # Already -1 to +1
            'positive': round(scores['pos'], 3),
            'negative': round(scores['neg'], 3),
            'neutral': round(scores['neu'], 3)
        }