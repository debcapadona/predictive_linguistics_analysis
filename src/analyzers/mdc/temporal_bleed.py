"""
temporal_bleed.py - Dimension 4: Temporal Bleed

Detects future events described in past tense or temporal contradictions
Uses LLM to identify linguistic markers of timeline distortion
Score: 0.0 (normal) to 1.0 (high temporal bleed)
"""

from dotenv import load_dotenv
load_dotenv()  

import os
from typing import Dict


class TemporalBleedAnalyzer:
    """Analyze temporal contradictions using LLM"""
    
    def __init__(self, provider: str = 'groq'):
        """
        Initialize temporal bleed analyzer
        
        Args:
            provider: 'groq', 'openai', or 'anthropic'
        """
        self.provider = provider
        self.available = False
        
        if provider == 'groq':
            try:
                from groq import Groq
                api_key = os.getenv('GROQ_API_KEY')
                if api_key:
                    self.client = Groq(api_key=api_key)
                    self.model = "llama-3.1-8b-instant"
                    self.available = True
                    print("✓ Groq API available for temporal bleed analysis")
                else:
                    print("⚠️  GROQ_API_KEY not set. Set with: export GROQ_API_KEY='your-key'")
            except ImportError:
                print("⚠️  Groq not installed. Install with: pip install groq")
        
        elif provider == 'openai':
            try:
                from openai import OpenAI
                api_key = os.getenv('OPENAI_API_KEY')
                if api_key:
                    self.client = OpenAI(api_key=api_key)
                    self.model = "gpt-3.5-turbo"
                    self.available = True
                    print("✓ OpenAI API available for temporal bleed analysis")
                else:
                    print("⚠️  OPENAI_API_KEY not set")
            except ImportError:
                print("⚠️  OpenAI not installed. Install with: pip install openai")
        
        else:
            print(f"⚠️  Unknown provider: {provider}")
    
    def score(self, title: str) -> Dict:
        """
        Analyze text for temporal bleeding/contradictions
        
        Args:
            title: Story title or text to analyze
            
        Returns:
            Dictionary with score and detected patterns
        """
        if not self.available or not title:
            return {
                'score': 0.0,
                'detected': False,
                'reasoning': 'LLM not available',
                'examples': []
            }
        
        # Construct prompt for LLM
        prompt = f"""Analyze this text for temporal contradictions or "temporal bleed" - where future events are described in past tense, or there are logical impossibilities in timing.

Examples of temporal bleed:
- "I already lost everything in 2026" (past tense + future year)
- "Remember when the grid went down next year" (past + future)
- "It happened last week in December 2025" (if current date is before that)

Text to analyze: "{title}"

Respond in this exact format:
SCORE: [0.0 to 1.0, where 0=no temporal bleed, 1=strong temporal bleed]
DETECTED: [yes/no]
REASONING: [one sentence explanation]
EXAMPLES: [list any specific phrases showing temporal bleed, or "none"]"""

        try:
            if self.provider == 'groq':
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    max_tokens=200
                )
                result = response.choices[0].message.content
            
            elif self.provider == 'openai':
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    max_tokens=200
                )
                result = response.choices[0].message.content
            
            # Parse response
            score = 0.0
            detected = False
            reasoning = "Unable to parse"
            examples = []
            
            for line in result.split('\n'):
                line = line.strip()
                if line.startswith('SCORE:'):
                    try:
                        score = float(line.split(':')[1].strip())
                        score = max(0.0, min(1.0, score))  # Clamp 0-1
                    except:
                        pass
                elif line.startswith('DETECTED:'):
                    detected = 'yes' in line.lower()
                elif line.startswith('REASONING:'):
                    reasoning = line.split(':', 1)[1].strip()
                elif line.startswith('EXAMPLES:'):
                    examples_text = line.split(':', 1)[1].strip()
                    if examples_text.lower() != 'none':
                        examples = [examples_text]
            
            return {
                'score': round(score, 3),
                'detected': detected,
                'reasoning': reasoning[:100],  # Truncate
                'examples': examples
            }
            
        except Exception as e:
            return {
                'score': 0.0,
                'detected': False,
                'reasoning': f'Error: {str(e)[:50]}',
                'examples': []
            }