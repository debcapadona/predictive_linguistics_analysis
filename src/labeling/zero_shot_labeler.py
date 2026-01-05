"""
zero_shot_labeler.py - Generate training labels using frontier model

Uses Claude API to score HN stories across 9 MDC dimensions.
Output becomes training data for BERT fine-tuning.
"""

import json
import time
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional
from anthropic import Anthropic

# Budget tracking
COST_PER_1K_INPUT = 0.003  # Claude Sonnet
COST_PER_1K_OUTPUT = 0.015


ZERO_SHOT_PROMPT = """You are an analytical labeling system for early-signal linguistic pattern detection.

Your task is to analyze the provided text and score it across the following 9 dimensions.
Each score must be a float between 0.0 and 1.0.

Use the operational definitions exactly as provided.
Do not infer intent, truth, or correctness.
Analyze language patterns only.

For each dimension:
- Provide a numeric score (0.0–1.0)
- Provide a brief justification (1–2 sentences max)

Text: "{text}"

Dimensions:

1. Temporal Bleed: Presence of future events described as if already occurring, inevitable, or retroactively resolved.
   0.0 = clean tense separation | 0.5 = vague inevitability | 1.0 = explicit future-as-past

2. Certainty Collapse: Shift from probabilistic language toward absolute certainty about outcomes.
   0.0 = uncertainty preserved | 0.5 = mixed modal language | 1.0 = deterministic framing dominates

3. Emotional Valence Shift: Strong deviation from baseline emotional tone toward collective euphoria, dread, anger, or resignation.
   0.0 = emotionally neutral | 0.5 = noticeable emotional lean | 1.0 = overwhelming shared mood

4. Agency Reversal: Narrative shift in who is perceived as holding power or initiating action.
   0.0 = stable agency | 0.5 = ambiguous or contested | 1.0 = explicit reversal

5. Novel Meme / Phrasing Explosion: Emergence of new phrases, slang, or symbolic language that appears coordinated, prophetic, or ritualized.
   0.0 = no novel language | 0.5 = weak emergence | 1.0 = strong memetic signature

6. Metaphor Cluster Density: Multiple actors independently adopting the same metaphorical framing.
   0.0 = literal language | 0.5 = occasional metaphor | 1.0 = dominant shared metaphor

7. Pronoun Flip: Shift from individual perspective (I/me/my) to collective identity language (we/us/our).
   0.0 = personal narrative | 0.5 = mixed pronouns | 1.0 = collective identity dominant

8. Sacred / Profane Ratio: Use of religious, spiritual, or blasphemous language in otherwise secular contexts.
   0.0 = secular language | 0.5 = metaphorical spiritual | 1.0 = overt sacred/profane framing

9. Time Compression Markers: Language indicating accelerated perception of time or collapsing timelines.
   0.0 = normal time framing | 0.5 = mild acceleration | 1.0 = extreme compression

Output format:
Return ONLY valid JSON with the following structure:

{"temporal_bleed": {"score": 0.0, "reason": ""}, "certainty_collapse": {"score": 0.0, "reason": ""}, "emotional_valence_shift": {"score": 0.0, "reason": ""}, "agency_reversal": {"score": 0.0, "reason": ""}, "novel_meme_explosion": {"score": 0.0, "reason": ""}, "metaphor_cluster_density": {"score": 0.0, "reason": ""}, "pronoun_flip": {"score": 0.0, "reason": ""}, "sacred_profane_ratio": {"score": 0.0, "reason": ""}, "time_compression_markers": {"score": 0.0, "reason": ""}}

Rules:
- Do not include markdown.
- Do not include explanations outside the JSON.
- Be conservative: score high only when signals are clearly present.
- Use the full 0.0-1.0 range with decimal precision (e.g., 0.15, 0.42, 0.78).
"""


class ZeroShotLabeler:
    """Generate MDC training labels using Claude API"""
    
    def __init__(self, db_path: str = "data/linguistic_predictor.db"):
        self.client = Anthropic()
        self.db_path = db_path
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        
    def label_text(self, text: str) -> Optional[Dict]:
        """Score a single text across all 9 dimensions"""
        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1024,
                messages=[{
                    "role": "user",
                    "content": ZERO_SHOT_PROMPT.replace("{text}", text[:4000])  # Truncate long texts
                }]
            )
            
            # Track tokens
            self.total_input_tokens += response.usage.input_tokens
            self.total_output_tokens += response.usage.output_tokens
            
            # Parse JSON response
            content = response.content[0].text.strip()
            
            # DEBUG - see what we're getting
            print(f"  DEBUG raw response: {content[:200]}...")
            
            # Handle markdown code blocks if model ignores rules
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
            
            return json.loads(content)
            
        except json.JSONDecodeError as e:
            print(f"JSON parse error: {e}")
            print(f"Raw response: {content[:500]}")
            return None
        except Exception as e:
            print(f"API error: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_sample_stories(self, n: int = 10, min_length: int = 100) -> List[Dict]:
        """Get random sample of stories for labeling"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get stories with enough text content
        cursor.execute("""
            SELECT s.id, s.title, 
                   COALESCE(s.title || ' ' || p.words, s.title) as full_text
            FROM stories s
            LEFT JOIN processed_text p ON s.id = p.story_id
            WHERE LENGTH(COALESCE(s.title || ' ' || p.words, s.title)) > ?
            ORDER BY RANDOM()
            LIMIT ?
        """, (min_length, n))
        
        stories = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return stories
    
    def estimate_cost(self) -> Dict:
        """Calculate current spend"""
        input_cost = (self.total_input_tokens / 1000) * COST_PER_1K_INPUT
        output_cost = (self.total_output_tokens / 1000) * COST_PER_1K_OUTPUT
        return {
            "input_tokens": self.total_input_tokens,
            "output_tokens": self.total_output_tokens,
            "input_cost": round(input_cost, 4),
            "output_cost": round(output_cost, 4),
            "total_cost": round(input_cost + output_cost, 4)
        }
    
    def run_labeling(self, n_stories: int = 10, delay: float = 0.5):
        """Label a batch of stories and save results"""
        print(f"Fetching {n_stories} stories...")
        stories = self.get_sample_stories(n_stories)
        print(f"Got {len(stories)} stories")
        
        results = []
        
        for i, story in enumerate(stories):
            print(f"\n[{i+1}/{len(stories)}] {story['title'][:60]}...")
            
            labels = self.label_text(story['full_text'])
            
            if labels:
                results.append({
                    "story_id": story['id'],
                    "title": story['title'],
                    "labels": labels
                })
                
                # Show a sample score
                tc = labels.get('time_compression_markers', {})
                print(f"  Time Compression: {tc.get('score', 'N/A')} - {tc.get('reason', '')[:50]}")
            else:
                print(f"  FAILED")
            
            # Rate limiting
            time.sleep(delay)
        
        # Summary
        cost = self.estimate_cost()
        print(f"\n{'='*60}")
        print(f"LABELING COMPLETE")
        print(f"{'='*60}")
        print(f"Stories labeled: {len(results)}/{len(stories)}")
        print(f"Total tokens: {cost['input_tokens']} in / {cost['output_tokens']} out")
        print(f"Total cost: ${cost['total_cost']}")
        print(f"{'='*60}")
        
        return results


# Save results to JSON for now (PostgreSQL integration later)
def save_results(results: List[Dict], output_path: str = "data/zero_shot_labels.json"):
    """Save labeling results to JSON"""
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"Saved {len(results)} results to {output_path}")


if __name__ == "__main__":
    import sys
    
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    
    print(f"Zero-Shot Labeler - Processing {n} stories")
    print("="*60)
    
    labeler = ZeroShotLabeler()
    results = labeler.run_labeling(n_stories=n)
    
    if results:
        save_results(results)