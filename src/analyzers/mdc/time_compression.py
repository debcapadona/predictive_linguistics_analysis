"""
time_compression.py - MDC Dimension 9: Time Compression

Detects acceleration panic and timeline compression language:
- "Happening so fast"
- "Can't keep up"
- "Years feel like days"
- Information overload signals

Collective perception of accelerating events.
"""

from typing import Dict


class TimeCompressionAnalyzer:
    """Detect time compression and acceleration panic in text"""
    
    # Speed/acceleration markers
    SPEED_MARKERS = {
        'so fast', 'too fast', 'happening fast', 'moving fast',
        'rapidly', 'accelerating', 'speeding up', 'breakneck',
        'lightning speed', 'at pace', 'unprecedented pace',
        'faster and faster', 'gaining speed', 'picking up speed'
    }
    
    # Timeline distortion markers
    TIMELINE_MARKERS = {
        'feels like years', 'feels like forever', 'time is speeding up',
        'days feel like weeks', 'weeks feel like months',
        'compressed timeline', 'time compression',
        'yesterday feels like', 'already feels like',
        'time flies', 'where did the time go'
    }
    
    # Overwhelm markers
    OVERWHELM_MARKERS = {
        "can't keep up", "can't follow", "barely keeping up",
        "struggling to keep up", "hard to keep up",
        'too much', 'overwhelming', 'overwhelmed',
        'drowning in', 'buried in', 'swamped',
        'information overload', 'too much information',
        'everything at once', 'all at once', 'happening at once',
        'nonstop', 'non-stop', 'relentless',
        "can't process", "can't absorb", 'head spinning'
    }
    
    # Temporal intensity boosters
    INTENSITY_MARKERS = {
        'exponential', 'exponentially',
        'insane', 'insanely', 'crazy', 'crazily',
        'unbelievable', 'unbelievably',
        'unprecedented', 'never seen',
        'breaking', 'record', 'historic'
    }
    
    def __init__(self):
        """Initialize time compression analyzer"""
        # Combine all marker sets for efficiency
        self.all_markers = (
            self.SPEED_MARKERS | 
            self.TIMELINE_MARKERS | 
            self.OVERWHELM_MARKERS | 
            self.INTENSITY_MARKERS
        )
        
        # Build lowercase lookup for case-insensitive matching
        self.marker_lookup = {m.lower(): m for m in self.all_markers}
    
    def analyze(self, text: str) -> Dict:
        """
        Analyze text for time compression signals
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with:
                - compression_score: 0.0-1.0 (normalized by text length)
                - speed_count: Speed/acceleration markers
                - timeline_count: Timeline distortion markers
                - overwhelm_count: Overwhelm markers
                - intensity_count: Intensity boosters
                - total_markers: Total compression markers
                - detected_phrases: List of actual phrases found
        """
        if not text:
            return self._empty_result()
        
        text_lower = text.lower()
        
        # Count each category
        speed_count = 0
        speed_phrases = []
        
        timeline_count = 0
        timeline_phrases = []
        
        overwhelm_count = 0
        overwhelm_phrases = []
        
        intensity_count = 0
        intensity_phrases = []
        
        # Check for each marker type
        for marker in self.SPEED_MARKERS:
            if marker.lower() in text_lower:
                speed_count += 1
                speed_phrases.append(marker)
        
        for marker in self.TIMELINE_MARKERS:
            if marker.lower() in text_lower:
                timeline_count += 1
                timeline_phrases.append(marker)
        
        for marker in self.OVERWHELM_MARKERS:
            if marker.lower() in text_lower:
                overwhelm_count += 1
                overwhelm_phrases.append(marker)
        
        for marker in self.INTENSITY_MARKERS:
            if marker.lower() in text_lower:
                intensity_count += 1
                intensity_phrases.append(marker)
        
        # Calculate totals
        total_markers = speed_count + timeline_count + overwhelm_count + intensity_count
        
        # Collect all detected phrases
        detected_phrases = speed_phrases + timeline_phrases + overwhelm_phrases + intensity_phrases
        
        # Calculate compression score
        # Normalize by text length (markers per 100 words)
        words = len(text.split())
        if words > 0:
            # Base score: markers per 100 words, capped at 1.0
            base_score = min(1.0, (total_markers / max(words, 1)) * 100)
            
            # Boost score if multiple categories present (stronger signal)
            categories_present = sum([
                speed_count > 0,
                timeline_count > 0,
                overwhelm_count > 0,
                intensity_count > 0
            ])
            
            category_multiplier = 1.0 + (categories_present * 0.1)  # Up to 1.4x
            
            compression_score = min(1.0, base_score * category_multiplier)
        else:
            compression_score = 0.0
        
        return {
            'compression_score': round(compression_score, 3),
            'speed_count': speed_count,
            'timeline_count': timeline_count,
            'overwhelm_count': overwhelm_count,
            'intensity_count': intensity_count,
            'total_markers': total_markers,
            'detected_phrases': detected_phrases,
            'categories_present': categories_present if words > 0 else 0
        }
    
    def _empty_result(self) -> Dict:
        """Return empty result for null/empty text"""
        return {
            'compression_score': 0.0,
            'speed_count': 0,
            'timeline_count': 0,
            'overwhelm_count': 0,
            'intensity_count': 0,
            'total_markers': 0,
            'detected_phrases': [],
            'categories_present': 0
        }


# Test functionality
if __name__ == "__main__":
    analyzer = TimeCompressionAnalyzer()
    
    # Test cases
    test_texts = [
        "Normal tech news about a product launch",
        "Everything is happening so fast I can't keep up",
        "AI is accelerating at unprecedented pace - feels like years compressed into weeks",
        "The market is moving too fast, overwhelming amount of information",
        "Exponentially faster development, time is speeding up, can barely process it all"
    ]
    
    print("="*80)
    print("TIME COMPRESSION ANALYZER - TEST CASES")
    print("="*80)
    
    for i, text in enumerate(test_texts, 1):
        result = analyzer.analyze(text)
        print(f"\nTest {i}: {text[:60]}...")
        print(f"  Compression Score: {result['compression_score']:.3f}")
        print(f"  Speed markers: {result['speed_count']}")
        print(f"  Timeline markers: {result['timeline_count']}")
        print(f"  Overwhelm markers: {result['overwhelm_count']}")
        print(f"  Intensity markers: {result['intensity_count']}")
        print(f"  Categories present: {result['categories_present']}")
        if result['detected_phrases']:
            print(f"  Detected: {', '.join(result['detected_phrases'][:5])}")
    
    print("\n" + "="*80)
    print("✓ Time Compression Analyzer ready!")
    print("="*80)