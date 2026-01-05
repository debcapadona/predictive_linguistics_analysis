"""
tension_analyzer_bert.py - Tension/Release scoring using pre-trained models

Replaces keyword matching with actual sentiment understanding.
Uses distilbert for sentiment + emotion model for nuance.

The score represents the STORY's tension level, which then gets
assigned to every word in that story during word tagging.

DISCOVERY MODE: Returns raw BERT outputs, no weighting assumptions.
"""

from transformers import pipeline
from typing import Dict
import warnings
warnings.filterwarnings("ignore")


class TensionAnalyzerBERT:
    """
    Analyze tension/release using pre-trained transformer models.
    
    Returns raw sentiment and emotion scores for discovery.
    """
    
    def __init__(self, device: str = "cpu"):
        """
        Initialize the analyzer with pre-trained models.
        
        Args:
            device: "cpu" or "cuda" for GPU acceleration
        """
        print("Loading tension analysis models...")
        
        # Sentiment: positive/negative (-1 to +1 scale)
        self.sentiment = pipeline(
            "sentiment-analysis",
            model="distilbert-base-uncased-finetuned-sst-2-english",
            device=device if device == "cuda" else -1
        )
        
        # Emotion: anger, disgust, fear, joy, neutral, sadness, surprise
        self.emotion = pipeline(
            "text-classification",
            model="j-hartmann/emotion-english-distilroberta-base",
            device=device if device == "cuda" else -1,
            top_k=None  # Return all emotion scores
        )
        
        print("✓ Models loaded")
    
    def score(self, text: str) -> Dict:
        """
        Calculate tension score for text.
        
        Args:
            text: The story title or comment text
            
        Returns:
            Dictionary with raw BERT outputs for discovery
        """
        if not text or len(text.strip()) < 3:
            return self._empty_result()
        
        # Truncate to model's max length (512 tokens ≈ 400 words)
        text = text[:1500]
        
        try:
            # Get sentiment
            sent_result = self.sentiment(text)[0]
            sentiment_label = sent_result['label']  # POSITIVE or NEGATIVE
            sentiment_conf = sent_result['score']
            
            # Convert to -1 to +1 scale
            sentiment_score = sentiment_conf if sentiment_label == "POSITIVE" else -sentiment_conf
            
            # Get emotions - raw scores, no weighting
            emotion_results = self.emotion(text)[0]
            emotions = {e['label']: round(e['score'], 3) for e in emotion_results}
            
            # Find dominant emotion
            dominant_emotion = max(emotions, key=emotions.get)
            
            return {
                'sentiment': sentiment_label,
                'sentiment_score': round(sentiment_score, 3),
                'confidence': round(sentiment_conf, 3),
                'dominant_emotion': dominant_emotion,
                'emotions': emotions  # All 7 raw: anger, disgust, fear, joy, neutral, sadness, surprise
            }
            
        except Exception as e:
            print(f"Error analyzing text: {e}")
            return self._empty_result()
    
    def _empty_result(self) -> Dict:
        """Return empty result for invalid input"""
        return {
            'sentiment': 'NEUTRAL',
            'sentiment_score': 0.0,
            'confidence': 0.0,
            'dominant_emotion': 'neutral',
            'emotions': {}
        }


# Quick test
if __name__ == "__main__":
    print("=" * 70)
    print("TENSION ANALYZER - BERT VERSION (DISCOVERY MODE)")
    print("=" * 70)
    
    analyzer = TensionAnalyzerBERT()
    
    test_cases = [
        "Apple announces record quarterly earnings",
        "Markets uncertain as Fed hints at rate changes",
        "BREAKING: Major earthquake strikes coastal region",
        "Scientists confirm breakthrough in cancer treatment",
        "Government warns of imminent economic crisis",
        "Community celebrates after missing child found safe",
        "Experts fear situation could escalate rapidly",
        "Deal reached, ending months of negotiations",
    ]
    
    for text in test_cases:
        result = analyzer.score(text)
        print(f"\n{text}")
        print(f"  Sentiment: {result['sentiment']} ({result['sentiment_score']:.3f})")
        print(f"  Dominant:  {result['dominant_emotion']}")
        print(f"  Emotions:  {result['emotions']}")
    
    print("\n" + "=" * 70)