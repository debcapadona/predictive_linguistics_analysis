"""
sacred_profane.py - MDC Dimension 8: Sacred/Profane Ratio

Detects religious/existential language in secular tech spaces:
- Religious language: "pray", "God help us", "miracle"
- Profane/doom language: "we're cooked", "it's over", "dead"
- Nihilism markers: collective despair, capitulation

Market bottoms = peak despair
Paradigm shifts = sacred language surge
"""

from typing import Dict


class SacredProfaneAnalyzer:
    """Detect religious/existential language and nihilism"""
    
    # Religious/sacred language
    SACRED_MARKERS = {
        'god', 'pray', 'prayer', 'praying', 'prayers',
        'miracle', 'miraculous', 'divine', 'holy',
        'blessed', 'blessing', 'faith', 'believe',
        'heaven', 'hell', 'salvation', 'save us',
        'lord', 'jesus', 'christ', 'amen',
        'sacred', 'spiritual', 'soul',
        'god willing', 'god help us', 'thank god',
        'oh god', 'my god', 'oh my god'
    }
    
    # Profane/doom language
    PROFANE_MARKERS = {
        'dead', 'dying', 'death', 'killed', 'killing',
        'doomed', 'doom', 'apocalypse', 'apocalyptic',
        'catastrophe', 'catastrophic', 'disaster',
        'collapse', 'collapsing', 'collapsed',
        'end times', 'the end', 'endgame',
        'finished', 'done for', 'game over',
        'no hope', 'hopeless', 'lost cause',
        'fuck', 'fucked', 'shit', 'damn', 'hell'
    }
    
    # Nihilism/capitulation markers
    NIHILISM_MARKERS = {
        "we're cooked", "it's over", "its over",
        "we're done", "it's done", "already over",
        "too late", "nothing matters", "pointless",
        "give up", "giving up", "gave up",
        "lost", "losing", "can't win",
        "no point", "what's the point", "why bother",
        "inevitable", "unavoidable", "inescapable",
        "resistance is futile", "fighting a losing battle",
        "accept defeat", "surrender", "capitulate"
    }
    
    # Despair/resignation markers
    DESPAIR_MARKERS = {
        'bleak', 'grim', 'dire', 'hopeless',
        'depressing', 'depressed', 'depression',
        'miserable', 'suffering', 'pain',
        'dark', 'darkness', 'black pill',
        'give in', 'giving in', 'acceptance',
        'resigned', 'resignation', 'fatalistic',
        'helpless', 'powerless', 'defeated'
    }
    
    def __init__(self):
        """Initialize sacred/profane analyzer"""
        # Combine all marker sets
        self.all_markers = (
            self.SACRED_MARKERS | 
            self.PROFANE_MARKERS | 
            self.NIHILISM_MARKERS | 
            self.DESPAIR_MARKERS
        )
    
    def analyze(self, text: str) -> Dict:
        """
        Analyze text for sacred/profane language
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with:
                - sacred_profane_score: -1.0 to +1.0 (profane to sacred)
                - sacred_count: Religious/sacred markers
                - profane_count: Profane/doom markers
                - nihilism_count: Nihilism/capitulation markers
                - despair_count: Despair/resignation markers
                - total_markers: Total markers found
                - ratio: (sacred - profane) / total (balance)
                - detected_phrases: List of actual phrases found
        """
        if not text:
            return self._empty_result()
        
        text_lower = text.lower()
        
        # Count each category
        sacred_count = 0
        sacred_phrases = []
        
        profane_count = 0
        profane_phrases = []
        
        nihilism_count = 0
        nihilism_phrases = []
        
        despair_count = 0
        despair_phrases = []
        
        # Check for each marker type
        for marker in self.SACRED_MARKERS:
            if marker in text_lower:
                sacred_count += 1
                sacred_phrases.append(marker)
        
        for marker in self.PROFANE_MARKERS:
            if marker in text_lower:
                profane_count += 1
                profane_phrases.append(marker)
        
        for marker in self.NIHILISM_MARKERS:
            if marker in text_lower:
                nihilism_count += 1
                nihilism_phrases.append(marker)
        
        for marker in self.DESPAIR_MARKERS:
            if marker in text_lower:
                despair_count += 1
                despair_phrases.append(marker)
        
        # Calculate totals
        # Profane includes doom + nihilism + despair (all negative)
        total_profane = profane_count + nihilism_count + despair_count
        total_markers = sacred_count + total_profane
        
        # Collect all detected phrases
        detected_phrases = sacred_phrases + profane_phrases + nihilism_phrases + despair_phrases
        
        # Calculate sacred/profane score
        if total_markers > 0:
            # Ratio from -1.0 (all profane) to +1.0 (all sacred)
            ratio = (sacred_count - total_profane) / total_markers
            
            # Absolute score (how much sacred/profane language overall)
            words = len(text.split())
            intensity = min(1.0, (total_markers / max(words, 1)) * 100)
            
            # Final score: ratio weighted by intensity
            sacred_profane_score = ratio * intensity
        else:
            ratio = 0.0
            sacred_profane_score = 0.0
        
        return {
            'sacred_profane_score': round(sacred_profane_score, 3),
            'sacred_count': sacred_count,
            'profane_count': profane_count,
            'nihilism_count': nihilism_count,
            'despair_count': despair_count,
            'total_markers': total_markers,
            'ratio': round(ratio, 3),
            'detected_phrases': detected_phrases
        }
    
    def _empty_result(self) -> Dict:
        """Return empty result for null/empty text"""
        return {
            'sacred_profane_score': 0.0,
            'sacred_count': 0,
            'profane_count': 0,
            'nihilism_count': 0,
            'despair_count': 0,
            'total_markers': 0,
            'ratio': 0.0,
            'detected_phrases': []
        }


# Test functionality
if __name__ == "__main__":
    analyzer = SacredProfaneAnalyzer()
    
    # Test cases
    test_texts = [
        "Normal tech news about a product launch",
        "Tech is dead, we're completely cooked, it's over",
        "Pray this works, we need a miracle to save us",
        "Market collapse, doom and gloom, nothing matters anymore, hopeless situation",
        "God help us, this is the end times, apocalyptic scenario, we're fucked"
    ]
    
    print("="*80)
    print("SACRED/PROFANE RATIO ANALYZER - TEST CASES")
    print("="*80)
    
    for i, text in enumerate(test_texts, 1):
        result = analyzer.analyze(text)
        print(f"\nTest {i}: {text[:60]}...")
        print(f"  Sacred/Profane Score: {result['sacred_profane_score']:+.3f}")
        print(f"  Ratio: {result['ratio']:+.3f} (-1=profane, +1=sacred)")
        print(f"  Sacred markers: {result['sacred_count']}")
        print(f"  Profane markers: {result['profane_count']}")
        print(f"  Nihilism markers: {result['nihilism_count']}")
        print(f"  Despair markers: {result['despair_count']}")
        if result['detected_phrases']:
            print(f"  Detected: {', '.join(result['detected_phrases'][:7])}")
    
    print("\n" + "="*80)
    print("✓ Sacred/Profane Analyzer ready!")
    print("="*80)