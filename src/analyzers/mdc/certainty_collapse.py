"""
certainty_collapse.py - Dimension 1: Certainty Collapse

Measures shift from uncertain language (if/maybe) to certain language (when/will)
Score: -1.0 (highly uncertain) to +1.0 (highly certain)
"""

from typing import Dict


class CertaintyCollapseAnalyzer:
    """Analyze certainty vs uncertainty in text"""
    
    # Linguistic markers
    UNCERTAINTY_MARKERS = {
        'if', 'maybe', 'possibly', 'perhaps', 'could', 'might',
        'uncertain', 'unclear', 'unsure', 'probably', 'likely'
    }
    
    CERTAINTY_MARKERS = {
        'when', 'will', 'definitely', 'certainly', 'already',
        'must', 'always', 'never', 'confirmed', 'proven',
        'guaranteed', 'surely', 'absolutely'
    }
    
    def score(self, words: str) -> Dict:
        """
        Calculate certainty collapse score for text
        
        Args:
            words: Pipe-separated words from processed text
            
        Returns:
            Dictionary with score and details
        """
        if not words:
            return {
                'score': 0.0,
                'uncertainty_count': 0,
                'certainty_count': 0,
                'total_markers': 0
            }
        
        word_list = words.split('|')
        word_set = set(word_list)
        
        # Count markers
        uncertainty_count = len(word_set & self.UNCERTAINTY_MARKERS)
        certainty_count = len(word_set & self.CERTAINTY_MARKERS)
        total_markers = uncertainty_count + certainty_count
        
        # Calculate score
        if total_markers == 0:
            score = 0.0
        else:
            # Range: -1 (all uncertain) to +1 (all certain)
            score = (certainty_count - uncertainty_count) / total_markers
        
        return {
            'score': round(score, 3),
            'uncertainty_count': uncertainty_count,
            'certainty_count': certainty_count,
            'total_markers': total_markers
        }