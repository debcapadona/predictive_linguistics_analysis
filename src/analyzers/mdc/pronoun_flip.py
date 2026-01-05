"""
pronoun_flip.py - Dimension 2: Pronoun Distribution

Measures distribution across first-person, third-person, and collective pronouns
Outputs 3 separate scores instead of binary
"""

from typing import Dict


class PronounFlipAnalyzer:
    """Analyze pronoun distribution across three categories"""
    
# Pronoun categories
    FIRST_PERSON = {
        'i', 'me', 'my', 'mine', 'myself'
    }
    
    THIRD_PERSON = {
        'he', 'she', 'him', 'her', 'his', 'hers', 'himself', 'herself'
    }
    
    COLLECTIVE = {
        'we', 'us', 'our', 'ours', 'ourselves',
        'they', 'them', 'their', 'theirs', 'themselves'
    }
    
    def score(self, words: str) -> Dict:
        """
        Calculate pronoun distribution scores
        
        Args:
            words: Pipe-separated words from processed text
            
        Returns:
            Dictionary with three separate scores
        """
        if not words:
            return {
                'first_person_score': 0.0,
                'third_person_score': 0.0,
                'collective_score': 0.0,
                'first_person_count': 0,
                'third_person_count': 0,
                'collective_count': 0,
                'total_pronouns': 0
            }
        
        word_list = words.split('|')
        
        # Count pronouns (include frequency)
        first_count = sum(1 for w in word_list if w in self.FIRST_PERSON)
        third_count = sum(1 for w in word_list if w in self.THIRD_PERSON)
        collective_count = sum(1 for w in word_list if w in self.COLLECTIVE)
        total_pronouns = first_count + third_count + collective_count
        
        # Calculate scores (percentage of total pronouns)
        if total_pronouns == 0:
            first_score = 0.0
            third_score = 0.0
            collective_score = 0.0
        else:
            first_score = first_count / total_pronouns
            third_score = third_count / total_pronouns
            collective_score = collective_count / total_pronouns
        
        return {
            'first_person_score': round(first_score, 3),
            'third_person_score': round(third_score, 3),
            'collective_score': round(collective_score, 3),
            'first_person_count': first_count,
            'third_person_count': third_count,
            'collective_count': collective_count,
            'total_pronouns': total_pronouns
        }