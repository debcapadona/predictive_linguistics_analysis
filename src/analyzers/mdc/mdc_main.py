"""
MDC (Multi-Dimensional Collapse) Orchestrator
Runs all 6 MDC analyzers on stories and generates classification vectors
"""

import sys
import re
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.analyzers.mdc.temporal_proximity import TemporalProximityAnalyzer
from src.analyzers.mdc.certainty_collapse import CertaintyCollapseAnalyzer
from src.analyzers.mdc.pronoun_flip import PronounFlipAnalyzer
from src.analyzers.mdc.emotional_valence import EmotionalValenceAnalyzer
from src.analyzers.mdc.temporal_bleed import TemporalBleedAnalyzer
from src.analyzers.mdc.time_compression import TimeCompressionAnalyzer
from src.analyzers.mdc.sacred_profane import SacredProfaneAnalyzer


class MDCOrchestrator:
    """Orchestrates all MDC dimension analyzers"""
    
    def __init__(self):
        """Initialize all analyzers"""
        self.proximity = TemporalProximityAnalyzer()
        self.certainty = CertaintyCollapseAnalyzer()
        self.pronoun = PronounFlipAnalyzer()
        self.valence = EmotionalValenceAnalyzer()
        self.temporal = TemporalBleedAnalyzer()
        self.time_comp = TimeCompressionAnalyzer()
        self.sacred = SacredProfaneAnalyzer()
    
    def _tokenize(self, text: str) -> str:
        """Convert text to pipe-separated words for analyzers"""
        # Split on whitespace and punctuation, keep only words
        words = re.findall(r'\b\w+\b', text.lower())
        return '|'.join(words)
        
    def vectorize_story(self, story_text: str, use_llm: bool = True) -> dict:
        """
        Run all 6 MDC analyzers on a story and return classification vector
        
        Args:
            story_text: The story title/text to analyze
            use_llm: Whether to use LLM for temporal bleed (costs money)
            
        Returns:
            dict with all 6 dimension scores plus metadata
        """
        results = {}
        
        # Tokenize for word-based analyzers
        tokenized = self._tokenize(story_text)
        
        # Dimension 1: Certainty Collapse
        certainty_result = self.certainty.score(tokenized)
        results['certainty_score'] = certainty_result['score']
        results['certainty_uncertain_count'] = certainty_result['uncertainty_count']
        results['certainty_certain_count'] = certainty_result['certainty_count']
        
        # Dimension 2: Pronoun Distribution
        pronoun_result = self.pronoun.score(tokenized)
        results['pronoun_first'] = pronoun_result['first_person_score']
        results['pronoun_third'] = pronoun_result['third_person_score']
        results['pronoun_collective'] = pronoun_result['collective_score']
        
        # Dimension 3: Emotional Valence (uses original text)
        valence_result = self.valence.score(story_text)
        results['valence_score'] = valence_result['score']
        results['valence_positive'] = valence_result['positive']
        results['valence_negative'] = valence_result['negative']
        results['valence_neutral'] = valence_result['neutral']
        
        # Dimension 4: Temporal Bleed (uses original text, LLM-based)
        if use_llm:
            temporal_result = self.temporal.score(story_text)
            results['temporal_bleed'] = temporal_result['score']
            results['temporal_detected'] = temporal_result['detected']
            results['temporal_reasoning'] = temporal_result.get('reasoning', '')
        else:
            results['temporal_bleed'] = 0.0
            results['temporal_detected'] = False
            results['temporal_reasoning'] = 'LLM disabled'
            
        # Dimension 5: Time Compression (uses original text)
        time_comp_result = self.time_comp.analyze(story_text)
        results['time_compression'] = time_comp_result['compression_score']
        results['time_comp_speed'] = time_comp_result['speed_count']
        results['time_comp_timeline'] = time_comp_result['timeline_count']
        results['time_comp_overwhelm'] = time_comp_result['overwhelm_count']
        results['time_comp_intensity'] = time_comp_result['intensity_count']
        
        # Dimension 6: Sacred/Profane Ratio (uses original text)
        sacred_result = self.sacred.analyze(story_text)
        results['sacred_profane'] = sacred_result['sacred_profane_score']
        results['sacred_count'] = sacred_result['sacred_count']
        results['profane_count'] = sacred_result['profane_count']
        results['nihilism_count'] = sacred_result['nihilism_count']
        results['despair_count'] = sacred_result['despair_count']

        # Dimension 7: Temporal Proximity
        proximity_result = self.proximity.analyze(story_text)
        results['temporal_proximity'] = proximity_result['proximity_score']
        results['proximity_category'] = proximity_result['category']
        results['proximity_immediate'] = proximity_result['immediate_count']
        results['proximity_impending'] = proximity_result['impending_count']
        results['proximity_near_term'] = proximity_result['near_term_count']
        results['proximity_long_term'] = proximity_result['long_term_count']
        results['proximity_amplified'] = proximity_result['amplified_count']
        results['proximity_hedged'] = proximity_result['hedged_count']
        
        # Future dimensions (placeholder)
        results['novel_meme'] = 0.0
        results['agency_reversal'] = 0.0
        results['metaphor_density'] = 0.0
        
        return results


def test_orchestrator():
    """Quick test of all 6 dimensions"""
    
    orchestrator = MDCOrchestrator()
    
    test_stories = [
        "Tesla stock will definitely crash tomorrow, we're all doomed",
        "Maybe the market might recover if things improve",
        "AI is evolving so fast I can't keep up, everything is accelerating",
        "The sacred duty of innovation meets the profane reality of capitalism",
        "I already saw this happen in 2026, trust me",
        "Startup raises $100M Series A to disrupt logistics"
    ]
    
    print("MDC Orchestrator Test - 6 Dimensions")
    print("=" * 80)
    
    for i, story in enumerate(test_stories, 1):
        print(f"\nStory {i}: {story[:60]}...")
        
        # Run with LLM for first 3, without for rest (save money)
        use_llm = i <= 3
        results = orchestrator.vectorize_story(story, use_llm=use_llm)
        
        print(f"  Temporal Proximity: {results['temporal_proximity']:.2f} ({results['proximity_category']}) [Amp:{results['proximity_amplified']}, Hedge:{results['proximity_hedged']}]")
        print(f"  Certainty:        {results['certainty_score']:+.2f}")
        print(f"  Pronoun (1st):    {results['pronoun_first']:.2f}")
        print(f"  Pronoun (coll):   {results['pronoun_collective']:.2f}")
        print(f"  Valence:          {results['valence_score']:+.2f}")
        print(f"  Temporal Bleed:   {results['temporal_bleed']:.2f}")
        print(f"  Time Compression: {results['time_compression']:.2f}")
        print(f"  Sacred/Profane:   {results['sacred_profane']:+.2f}")
        

if __name__ == "__main__":
    test_orchestrator()