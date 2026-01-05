# Linguistic Predictor Documentation

**Project:** Multi-Dimensional Collapse (MDC) System  
**Purpose:** Detect linguistic patterns that may predict future events  
**Status:** Phase 1 Complete (Proof of Concept)  
**Data:** 197,496 Hacker News posts (Jan 2024 - Dec 2024)

---

## Documentation Structure

### 1. [Project Overview](PROJECT_OVERVIEW.md)
- Executive summary
- Motivation and background
- System architecture
- Key results

### 2. [Technical Specifications](TECHNICAL_SPECS.md)
- Data sources and collection
- BERT dimension definitions
- Topic taxonomy structure
- Database schema

### 3. [Methodology](METHODOLOGY.md)
- BERTopic topic discovery
- Zero-shot labeling with Claude API
- Statistical validation approach
- Baseline establishment

### 4. [Results & Findings](RESULTS.md)
- Reddit API Blackout case study
- Statistical significance tests
- Cluster analysis
- Limitations and caveats

### 5. [User Guide](USER_GUIDE.md)
- How to run the system
- Interpreting results
- Adding new data sources
- Extending dimensions

### 6. [API Reference](API_REFERENCE.md)
- Database schema
- Key functions
- Data formats
- Tool interfaces

### 7. [Project Diary](project_diary.md)
- Development timeline
- Key decisions
- Learnings and pivots
- Future roadmap

---

## Quick Start
```bash
# Set up environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run analysis on existing data
python scripts/analyze_topic_dimensions.py

# Visualize results
python scripts/visualize_results.py
```

## Key Files

- `data/topic_dimension_correlations.csv` - Main results
- `data/taxonomy_structure.json` - Topic hierarchy
- `data/reddit_event_statistical_tests.csv` - Validation stats
- `visualizations/` - All generated charts

## Portfolio Highlights

**What Works:**
- ✅ Detected Reddit API Blackout with 34.7% agency_reversal spike (p < 0.0001)
- ✅ Discovered 67 natural topics from 197k HN posts
- ✅ Built 3-tier hierarchical taxonomy (6 domains, 25 categories, 67 topics)
- ✅ Labeled 163,512 comments with 53,479 topic assignments
- ✅ Established baseline distributions across full year

**Limitations:**
- ⚠️ Single data source (Hacker News only)
- ⚠️ Limited predictive validation (1 confirmed signal)
- ⚠️ Sample size concerns on peak detection day (220 stories)
- ⚠️ Needs multi-platform validation

**Next Steps:**
- 🔄 Multi-source validation (Reddit scraping in progress)
- 📊 More event case studies
- 🔮 Proper train/test split for prediction
- 📈 Real-time monitoring system