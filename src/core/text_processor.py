"""
text_processor.py - Core module for text processing

Provides consistent text cleaning, tokenization, and feature extraction
"""

import re
from collections import Counter
from typing import List, Dict, Tuple
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

# Ensure NLTK data is available
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)

try:
    nltk.data.find('tokenizers/punkt_tab')
except LookupError:
    nltk.download('punkt_tab', quiet=True)

class TextProcessor:
    """Process and analyze text data"""
    
    def __init__(self, min_word_length: int = 3, remove_stopwords: bool = True):
        """
        Initialize text processor
        
        Args:
            min_word_length: Minimum length for words to keep
            remove_stopwords: Whether to remove stopwords
        """
        self.min_word_length = min_word_length
        self.remove_stopwords = remove_stopwords
        self.stop_words = set(stopwords.words('english')) if remove_stopwords else set()
    
    def clean_text(self, text: str) -> str:
        """
        Clean text
        
        Args:
            text: Raw text
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'http\S+|www\S+', '', text)
        
        # Remove special characters but keep spaces and basic punctuation
        text = re.sub(r'[^a-z0-9\s]', ' ', text)
        
        # Normalize whitespace
        text = ' '.join(text.split())
        
        return text
    
    def tokenize(self, text: str) -> List[str]:
        """
        Tokenize text into words
        
        Args:
            text: Cleaned text
            
        Returns:
            List of tokens
        """
        if not text:
            return []
        
        # Tokenize
        tokens = word_tokenize(text)
        
        # Filter: alphabetic, minimum length, no stopwords
        tokens = [
            word for word in tokens
            if word.isalpha() 
            and len(word) >= self.min_word_length
            and (not self.remove_stopwords or word not in self.stop_words)
        ]
        
        return tokens
    
    def extract_ngrams(self, tokens: List[str], n: int = 2) -> List[str]:
        """
        Extract n-grams from tokens
        
        Args:
            tokens: List of tokens
            n: N-gram size (2 for bigrams, 3 for trigrams)
            
        Returns:
            List of n-grams as strings
        """
        if len(tokens) < n:
            return []
        
        ngrams = []
        for i in range(len(tokens) - n + 1):
            ngram = ' '.join(tokens[i:i+n])
            ngrams.append(ngram)
        
        return ngrams
    
    def process_text(self, text: str) -> Dict[str, any]:
        """
        Complete text processing pipeline
        
        Args:
            text: Raw text
            
        Returns:
            Dictionary with processed components
        """
        # Clean
        cleaned = self.clean_text(text)
        
        # Tokenize
        tokens = self.tokenize(cleaned)
        
        # Extract n-grams
        bigrams = self.extract_ngrams(tokens, n=2)
        trigrams = self.extract_ngrams(tokens, n=3)
        
        return {
            'original': text,
            'cleaned': cleaned,
            'tokens': tokens,
            'bigrams': bigrams,
            'trigrams': trigrams,
            'token_count': len(tokens)
        }
    
    @staticmethod
    def count_words(items: List[Dict], word_field: str = 'tokens') -> Counter:
        """
        Count word frequencies across items
        
        Args:
            items: List of processed items
            word_field: Field name containing words
            
        Returns:
            Counter object with word frequencies
        """
        all_words = []
        for item in items:
            words = item.get(word_field, [])
            if isinstance(words, str):
                words = words.split('|')
            all_words.extend(words)
        
        return Counter(all_words)
    
    @staticmethod
    def get_top_words(items: List[Dict], n: int = 100, 
                     word_field: str = 'tokens') -> List[Tuple[str, int]]:
        """
        Get top N words by frequency
        
        Args:
            items: List of processed items
            n: Number of top words
            word_field: Field containing words
            
        Returns:
            List of (word, count) tuples
        """
        word_counts = TextProcessor.count_words(items, word_field)
        return word_counts.most_common(n)