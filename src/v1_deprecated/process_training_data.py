# text_processor.py - Clean and process text data for analysis
# Creates three versions: original, words, and phrases

import csv
import re
from collections import Counter
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Download required NLTK data (run once)
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')
    nltk.download('punkt')
    nltk.download('punkt_tab')

class TextProcessor:
    """
    Processes text data for linguistic analysis
    Creates three versions: original, words, and phrases
    """
    
    def __init__(self):
        """Initialize the processor with stopwords"""
        self.stop_words = set(stopwords.words('english'))
        print("Text Processor initialized")
        print(f"Loaded {len(self.stop_words)} stopwords")
    
    def clean_text(self, text):
        """
        Basic text cleaning
        
        Args:
            text: Raw text string
            
        Returns:
            Cleaned text string
        """
        if not text:
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'http\S+|www\.\S+', '', text)
        
        # Remove special characters but keep spaces and basic punctuation
        text = re.sub(r'[^\w\s\'-]', ' ', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def tokenize(self, text):
        """
        Split text into words (tokens)
        
        Args:
            text: Cleaned text string
            
        Returns:
            List of word tokens
        """
        tokens = word_tokenize(text)
        
        # Keep only alphabetic tokens (no numbers, no punctuation)
        tokens = [token for token in tokens if token.isalpha()]
        
        # Keep only tokens longer than 2 characters
        tokens = [token for token in tokens if len(token) > 2]
        
        return tokens
    
    def remove_stopwords(self, tokens):
        """
        Remove common stopwords
        
        Args:
            tokens: List of word tokens
            
        Returns:
            List of tokens with stopwords removed
        """
        return [token for token in tokens if token not in self.stop_words]
    
    def create_ngrams(self, tokens, n=2):
        """
        Create n-grams (phrases) from tokens
        
        Args:
            tokens: List of word tokens
            n: Size of n-gram (2 = bigrams, 3 = trigrams)
            
        Returns:
            List of n-gram strings
        """
        ngrams = []
        for i in range(len(tokens) - n + 1):
            ngram = ' '.join(tokens[i:i+n])
            ngrams.append(ngram)
        return ngrams
    
    def process_title(self, title):
        """
        Process a single title and return all three versions
        
        Args:
            title: Raw title string
            
        Returns:
            Dictionary with original, words, and phrases
        """
        # 1. Original (keep for context)
        original = title
        
        # 2. Clean and tokenize
        cleaned = self.clean_text(title)
        all_tokens = self.tokenize(cleaned)
        
        # 3. Words (stopwords removed)
        words = self.remove_stopwords(all_tokens)
        
        # 4. Phrases (bigrams from all tokens, including stopwords)
        bigrams = self.create_ngrams(all_tokens, n=2)
        
        # 5. Phrases (trigrams from all tokens)
        trigrams = self.create_ngrams(all_tokens, n=3)
        
        return {
            'original': original,
            'cleaned': cleaned,
            'all_tokens': all_tokens,
            'words': words,
            'bigrams': bigrams,
            'trigrams': trigrams
        }
    
    def process_csv(self, input_file, output_file):
        """
        Process all titles from CSV and create processed output
        
        Args:
            input_file: Path to input CSV
            output_file: Path to output CSV
        """
        print(f"\nProcessing: {input_file}")
        print("=" * 60)
        
        processed_stories = []
        
        # Read input CSV
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            stories = list(reader)
        
        print(f"Found {len(stories)} stories to process")
        
        # Process each story
        for i, story in enumerate(stories, 1):
            if i % 100 == 0:
                print(f"Processed {i}/{len(stories)} stories...")
            
            title = story.get('title', '')
            
            # Process the title
            processed = self.process_title(title)
            
            # Combine with original story data
            processed_story = {
                'id': story.get('id', ''),
                'original_title': processed['original'],
                'cleaned_title': processed['cleaned'],
                'created_at': story.get('created_at', ''),
                'points': story.get('points', 0),
                'num_comments': story.get('num_comments', 0),
                'words': '|'.join(processed['words']),  # Join with | separator
                'bigrams': '|'.join(processed['bigrams']),
                'trigrams': '|'.join(processed['trigrams']),
                'word_count': len(processed['words'])
            }
            
            processed_stories.append(processed_story)
        
        # Write to output CSV
        print(f"\nSaving to: {output_file}")
        
        fieldnames = [
            'id', 
            'original_title', 
            'cleaned_title', 
            'created_at', 
            'points', 
            'num_comments',
            'words', 
            'bigrams', 
            'trigrams',
            'word_count'
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(processed_stories)
        
        print(f"✅ Processed {len(processed_stories)} stories")
        
        return processed_stories
    
    def show_sample(self, processed_stories, n=3):
        """
        Show sample processed stories
        
        Args:
            processed_stories: List of processed story dictionaries
            n: Number of samples to show
        """
        print("\n" + "=" * 60)
        print(f"SAMPLE PROCESSED STORIES (First {n})")
        print("=" * 60)
        
        for i, story in enumerate(processed_stories[:n], 1):
            print(f"\n{i}. Original: {story['original_title']}")
            print(f"   Cleaned:  {story['cleaned_title']}")
            print(f"   Words:    {story['words'][:80]}...")
            print(f"   Bigrams:  {story['bigrams'][:80]}...")
            print(f"   Count:    {story['word_count']} words")

def analyze_word_frequencies(processed_stories):
    """
    Analyze word frequencies across all stories
    
    Args:
        processed_stories: List of processed story dictionaries
    """
    print("\n" + "=" * 60)
    print("WORD FREQUENCY ANALYSIS")
    print("=" * 60)
    
    # Collect all words
    all_words = []
    for story in processed_stories:
        words = story['words'].split('|') if story['words'] else []
        all_words.extend(words)
    
    # Count frequencies
    word_freq = Counter(all_words)
    
    print(f"\nTotal words: {len(all_words)}")
    print(f"Unique words: {len(word_freq)}")
    
    print("\nTop 20 Most Frequent Words:")
    for word, count in word_freq.most_common(20):
        print(f"  {word:20s} : {count:4d}")
    
    # Analyze bigrams
    all_bigrams = []
    for story in processed_stories:
        bigrams = story['bigrams'].split('|') if story['bigrams'] else []
        all_bigrams.extend(bigrams)
    
    bigram_freq = Counter(all_bigrams)
    
    print("\nTop 20 Most Frequent Phrases (Bigrams):")
    for phrase, count in bigram_freq.most_common(20):
        if phrase:  # Skip empty strings
            print(f"  {phrase:30s} : {count:4d}")

# Main execution
if __name__ == "__main__":
    print("=" * 60)
    print("TEXT PROCESSOR - TRAINING DATA")
    print("=" * 60)
    
    # Initialize processor
    processor = TextProcessor()
    
    # Process the historical data
    input_file = 'data/raw/hackernews_training.csv'
    output_file = 'data/processed/hackernews_training_processed.csv'
    
    # Process all stories
    processed_stories = processor.process_csv(input_file, output_file)
    
    # Show samples
    processor.show_sample(processed_stories, n=5)
    
    # Analyze frequencies
    analyze_word_frequencies(processed_stories)
    
    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)