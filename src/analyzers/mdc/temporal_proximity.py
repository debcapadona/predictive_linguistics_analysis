"""
Temporal Proximity Analyzer
Detects urgency markers indicating how imminent predicted events feel
Plus intensity modifiers (amplifiers/hedges) near temporal markers
"""

from typing import Dict, List
import re


class TemporalProximityAnalyzer:
    """Analyze temporal urgency/proximity markers in text"""
    
    IMMEDIATE = {
        'now', 'today', 'currently', 'right now', 'at this moment',
        'as we speak', 'this instant', 'happening now', 'ongoing',
        'tomorrow', 'tonight', 'this morning', 'this afternoon'
    }
    
    IMPENDING = {
        'soon', 'about to', 'coming', 'imminent', 'any day now',
        'this week', 'in days', 'very soon', 'any minute',
        'on the verge', 'brink', 'edge of', 'incoming'
    }
    
    NEAR_TERM = {
        'next month', 'next quarter', 'next week', 'next year',
        'this quarter', 'this month', 'this week', 'this year',
        'coming months', 'coming weeks', 'coming quarters',
        'few weeks', 'few months', 'near future', 'short term',
        'by summer', 'by fall', 'by winter', 'by spring',
        'Q1', 'Q2', 'Q3', 'Q4', 'q1', 'q2', 'q3', 'q4'
    }
    
    LONG_TERM = {
        'next year', 'eventually', 'someday', 'long term',
        'in the future', 'down the road', 'years from now',
        'decade', 'decades'
    }
    
    # Intensity modifiers
    AMPLIFIERS = {
        'very', 'extremely', 'definitely', 'absolutely', 'certainly',
        'really', 'truly', 'clearly', 'obviously', 'undoubtedly',
        'inevitably', 'surely', 'guaranteed'
    }
    
    HEDGES = {
        'might', 'maybe', 'possibly', 'could', 'potentially',
        'perhaps', 'probably', 'likely', 'may', 'seems',
        'appears', 'suggests', 'indicates'
    }
    
    def __init__(self):
        """Initialize analyzer"""
        pass
    
    def analyze(self, text: str) -> Dict:
        """
        Analyze temporal proximity in text
        
        Args:
            text: Text to analyze
            
        Returns:
            Dict with proximity score and marker counts
        """
        if not text or not text.strip():
            return self._empty_result()
        
        text_lower = text.lower()
        words = text_lower.split()
        
        # Count markers in each category
        immediate_count = sum(1 for marker in self.IMMEDIATE if marker in text_lower)
        impending_count = sum(1 for marker in self.IMPENDING if marker in text_lower)
        near_term_count = sum(1 for marker in self.NEAR_TERM if marker in text_lower)
        long_term_count = sum(1 for marker in self.LONG_TERM if marker in text_lower)
        
        # Find detected markers for debugging
        detected = []
        if immediate_count > 0:
            detected.extend([m for m in self.IMMEDIATE if m in text_lower])
        if impending_count > 0:
            detected.extend([m for m in self.IMPENDING if m in text_lower])
        if near_term_count > 0:
            detected.extend([m for m in self.NEAR_TERM if m in text_lower])
        if long_term_count > 0:
            detected.extend([m for m in self.LONG_TERM if m in text_lower])
        
        # Check for year mentions (2024, 2025, etc.)
        year_pattern = r'\b(20\d{2}|203\d)\b'
        years = re.findall(year_pattern, text)
        if years:
            # Convert to proximity based on year
            for year_str in years:
                year = int(year_str)
                if year == 2024:
                    immediate_count += 1
                    detected.append(f"year:{year}")
                elif year == 2025:
                    impending_count += 1
                    detected.append(f"year:{year}")
                elif year <= 2027:
                    near_term_count += 1
                    detected.append(f"year:{year}")
                else:
                    long_term_count += 1
                    detected.append(f"year:{year}")
        
        # Calculate proximity score (highest urgency wins)
        if immediate_count > 0:
            proximity_score = 1.0
            category = 'immediate'
        elif impending_count > 0:
            proximity_score = 0.66
            category = 'impending'
        elif near_term_count > 0:
            proximity_score = 0.33
            category = 'near-term'
        elif long_term_count > 0:
            proximity_score = 0.0
            category = 'long-term'
        else:
            proximity_score = 0.5  # No markers = neutral/ambiguous
            category = 'unspecified'
        
        total_markers = immediate_count + impending_count + near_term_count + long_term_count
        
        # Detect intensity modifiers (check whole text, not just near markers)
        amplified_phrases = []
        hedged_phrases = []
        
        # Simple approach: if we have temporal markers AND modifiers, capture context
        has_temporal = total_markers > 0
        
        if has_temporal:
            # Look for amplifiers in the text
            for amp in self.AMPLIFIERS:
                if amp in words:
                    # Find context around amplifier
                    idx = words.index(amp)
                    context = ' '.join(words[max(0, idx-2):min(len(words), idx+4)])
                    amplified_phrases.append(context)
            
            # Look for hedges in the text
            for hedge in self.HEDGES:
                if hedge in words:
                    # Find context around hedge
                    idx = words.index(hedge)
                    context = ' '.join(words[max(0, idx-2):min(len(words), idx+4)])
                    hedged_phrases.append(context)
        
        return {
            'proximity_score': round(proximity_score, 3),
            'category': category,
            'immediate_count': immediate_count,
            'impending_count': impending_count,
            'near_term_count': near_term_count,
            'long_term_count': long_term_count,
            'total_markers': total_markers,
            'detected_markers': detected[:10],  # Limit for readability
            'amplified_count': len(set(amplified_phrases)),
            'hedged_count': len(set(hedged_phrases)),
            'amplified_phrases': list(set(amplified_phrases))[:5],
            'hedged_phrases': list(set(hedged_phrases))[:5]
        }
    
    def _empty_result(self) -> Dict:
        """Return empty result for null/empty text"""
        return {
            'proximity_score': 0.5,
            'category': 'unspecified',
            'immediate_count': 0,
            'impending_count': 0,
            'near_term_count': 0,
            'long_term_count': 0,
            'total_markers': 0,
            'detected_markers': [],
            'amplified_count': 0,
            'hedged_count': 0,
            'amplified_phrases': [],
            'hedged_phrases': []
        }


# Test functionality
if __name__ == "__main__":
    analyzer = TemporalProximityAnalyzer()
    
    test_cases = [
        "Market crash happening right now!",
        "AI will definitely surpass humans very soon",
        "Next quarter earnings might disappoint",
        "In 2030 we'll see major changes",
        "Stock could possibly recover eventually",
        "Everything is absolutely about to collapse any day now",
        "Maybe things will improve today",
        "Crash is clearly imminent and coming fast"
    ]
    
    print("Temporal Proximity Test (with Intensity Modifiers)")
    print("=" * 70)
    
    for text in test_cases:
        result = analyzer.analyze(text)
        print(f"\nText: {text}")
        print(f"  Score: {result['proximity_score']:.2f} ({result['category']})")
        print(f"  Markers: {result['detected_markers']}")
        print(f"  Amplified: {result['amplified_count']} - {result['amplified_phrases']}")
        print(f"  Hedged: {result['hedged_count']} - {result['hedged_phrases']}")