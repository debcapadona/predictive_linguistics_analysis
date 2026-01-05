# Multi-Dimensional Collapse (MDC) System
## Linguistic Event Detection Through Forum Analysis

**Author:** Debra "Deb" Capadona  
**Project Duration:** December 2024  
**Status:** Phase 1 Complete - Proof of Concept  
**Purpose:** Portfolio demonstration for Senior Technical Program Manager roles

---

## Executive Summary

The MDC system analyzes online forum discussions to detect linguistic patterns that may signal emerging events. Inspired by the WebBot project's early 2000s methodology, this modern implementation uses BERT embeddings, unsupervised topic modeling, and statistical validation to identify shifts in collective language that correlate with real-world events.

**Phase 1 analyzed 197,496 Hacker News posts** (January-December 2024) across nine linguistic dimensions, successfully detecting the June 2024 Reddit API Blackout with statistically significant signals (p < 0.0001).

---

## System Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                     DATA COLLECTION                         │
│  • Hacker News API scraper (197k posts, Jan-Dec 2024)      │
│  • Reddit API scraper (in progress, 10 subreddits)         │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                   TOPIC DISCOVERY                           │
│  • BERTopic: Automated topic extraction                    │
│  • 67 topics discovered via BERT + HDBSCAN clustering      │
│  • Claude API: Hierarchical taxonomy organization          │
│    - Tier 1: 6 domains (Technology, Science, etc.)         │
│    - Tier 2: 25 categories (Programming, Hardware, etc.)   │
│    - Tier 3: 67 specific topics                            │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│              DIMENSIONAL ANALYSIS (9 BERT Models)           │
│  1. Emotional Valence Shift    6. Pronoun Flip             │
│  2. Temporal Bleed             7. Agency Reversal          │
│  3. Certainty Collapse         8. Metaphor Cluster Density │
│  4. Time Compression           9. Novel Meme Explosion     │
│  5. Sacred/Profane Ratio                                    │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                  LABELING & ANALYSIS                        │
│  • 163,512 comments labeled with topics                    │
│  • 53,479 topic assignments (confidence scored)            │
│  • 244,319 word-label pairs (propagated from comments)     │
│  • 54,883 unique words with topic associations             │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│              STATISTICAL VALIDATION                         │
│  • Baseline establishment (full year 2024)                 │
│  • Event detection with p-values & effect sizes            │
│  • Anomaly detection (95th percentile threshold)           │
│  • Control period comparison                               │
└─────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

**Core Infrastructure:**
- **Database:** PostgreSQL (Docker), Alembic migrations
- **Topic Modeling:** BERTopic (BERT + HDBSCAN + UMAP)
- **NLP:** BERT embeddings (sentence-transformers)
- **Clustering:** HDBSCAN, UMAP dimensionality reduction
- **APIs:** Claude 3.5 Sonnet (zero-shot labeling, taxonomy)
- **Analysis:** pandas, scipy, numpy, scikit-learn
- **Visualization:** matplotlib, seaborn

**Development Tools:**
- Python 3.11, Docker, Git
- VS Code, Alembic (DB migrations)
- Great Expectations (data validation)

---

## Key Results

### 1. Topic Discovery & Taxonomy

**Discovered 67 natural topics** from 197k Hacker News posts using BERTopic:
- Automated clustering revealed discussion themes
- Organized into 3-tier hierarchy (6→25→67)
- Distribution:
  - Technology: 21 topics (31%)
  - Culture & Lifestyle: 12 topics (18%)
  - Science & Environment: 13 topics (19%)
  - Society & Politics: 9 topics (13%)
  - Media & Entertainment: 9 topics (13%)
  - Transportation: 3 topics (4%)

### 2. Event Detection: Reddit API Blackout (June 12, 2024)

**Statistically significant dimensional shifts detected:**

| Dimension | Baseline | Event Window | Change | P-Value | Effect Size (Cohen's d) |
|-----------|----------|--------------|--------|---------|------------------------|
| Agency Reversal | 0.0852 | 0.1148 | **+34.7%** | <0.0001 | 0.288 |
| Emotional Valence | 0.2011 | 0.2386 | +18.7% | <0.0001 | 0.237 |
| Certainty Collapse | 0.1740 | 0.2041 | +17.3% | <0.0001 | 0.222 |
| Time Compression | 0.0870 | 0.0991 | +13.9% | <0.0001 | 0.107 |
| Temporal Bleed | 0.1123 | 0.1244 | +10.8% | <0.0001 | 0.108 |

**All dimensions:**
- Exceeded 5 random control periods (anomalous)
- Showed statistical significance (p < 0.0001)
- Matched expected psychological patterns (loss of control, uncertainty)

### 3. Predictive Signal Detection

**Certainty Collapse anomaly detected 2 days BEFORE event:**
- June 10, 2024: Score of 0.2262 (97.9th percentile)
- Exceeded 95th percentile threshold (0.2123)
- **2-day advance warning**

**Caveats:**
- Only 220 stories on June 10 (small sample)
- Only 1 of 5 dimensions showed predictive signal
- Requires validation across more events
- May be statistical noise rather than true prediction

### 4. Cluster Analysis

**Three natural discussion clusters identified:**
- **Cluster 0** (5 topics): General discussions, moderate emotional content
- **Cluster 1** (9 topics): Engaged discussions, higher emotional/temporal markers
- **Cluster 2** (32 topics, mostly Technology): Low emotional, calm technical discussions
- **Outliers** (21 topics): Crisis topics (Israel-Gaza, China-Taiwan, TikTok ban)

---

## Domain-Level Patterns

**Society & Politics** consistently shows highest crisis dimensions:
- Emotional valence: 0.294 (vs 0.149 for Technology)
- Certainty collapse: 0.264 (2x higher than Technology)
- Agency reversal: 0.175 (4x higher than Technology)

**Technology discussions** show minimal emotional markers:
- Lowest across all 9 dimensions
- Focus on factual, technical content
- Less personal/emotional language

**Interpretation:** HN users discuss tech calmly, react emotionally to politics/policy.

---

## Validation Approach

### Statistical Rigor

**Baseline Establishment:**
- Full year 2024 daily scores (339 days × 5 dimensions = 1,695 observations)
- Calculated percentile distributions (50th, 75th, 90th, 95th, 99th)
- 95th percentile = anomaly threshold

**Event Testing:**
- T-tests for statistical significance (event vs baseline)
- Cohen's d for effect sizes
- Control period comparison (5 random weeks)
- Sample size tracking (min 200 stories/day for stability)

**Limitations Acknowledged:**
- Single data source (Hacker News only)
- Small sample on peak detection day (220 stories)
- Limited event validation (1 case study)
- Needs multi-platform confirmation

### In-Progress: Multi-Platform Validation

**Reddit data collection (ongoing):**
- 10 subreddits × full 2024 (estimated 5-7 days)
- Cross-platform validation of Reddit API Blackout
- Independent signal confirmation

---

## Business Value Proposition

### Use Cases

**1. Corporate Communications & PR**
- Early warning of brand reputation crises
- Monitor sentiment shifts around product launches
- Detect emerging controversies before mainstream media

**2. Investment Research**
- Sentiment tracking for tech stocks
- Detect community sentiment before market moves
- Monitor crypto/fintech discussion patterns

**3. Policy & Governance**
- Track public opinion on regulatory changes
- Detect growing concerns around tech policy
- Monitor community reactions to government actions

**4. Product Management**
- Detect user frustration before it becomes crisis
- Monitor competitive threat discussions
- Track emerging feature requests/complaints

### Competitive Advantages

- **Data-driven:** Automated topic discovery vs manual classification
- **Statistical rigor:** P-values, effect sizes, baseline comparison
- **Interpretable:** Clear dimension definitions, topic hierarchies
- **Scalable:** Designed for multi-platform expansion
- **Transparent:** Open methodology, documented limitations

---

## Portfolio Demonstration

### What This Project Demonstrates

**Technical Leadership:**
- ✅ Modern ML/NLP pipeline (BERT, clustering, APIs)
- ✅ Database architecture (PostgreSQL, migrations, indexing)
- ✅ Statistical validation (hypothesis testing, effect sizes)
- ✅ Data engineering (ETL, batch processing, 197k records)

**Program Management:**
- ✅ Phased approach (MVP → Validation → Scale)
- ✅ Risk management (documented limitations)
- ✅ Stakeholder communication (executive summary, technical docs)
- ✅ Deadline delivery (Phase 1 complete by Dec 31)

**Business Acumen:**
- ✅ Use case identification (PR, investment, policy)
- ✅ Value proposition clarity
- ✅ Honest about limitations (not overselling)
- ✅ Roadmap for improvement (Phase 2 multi-platform)

**Analytical Rigor:**
- ✅ Proper statistical testing
- ✅ Baseline establishment
- ✅ Control periods
- ✅ Acknowledges uncertainty

---

## Limitations & Future Work

### Current Limitations

**Data:**
- ⚠️ Single source (Hacker News only)
- ⚠️ Tech-focused demographic (not representative)
- ⚠️ English-only
- ⚠️ US-centric discussions

**Validation:**
- ⚠️ One event case study (Reddit API Blackout)
- ⚠️ Predictive signal unconfirmed (may be noise)
- ⚠️ No train/test split (all data used for exploration)
- ⚠️ Sample size concerns (220 stories on peak day)

**Methodology:**
- ⚠️ No causal claims (correlation only)
- ⚠️ Retrofit risk (analyzing known events)
- ⚠️ No blind prediction test
- ⚠️ Dimension definitions subjective

### Phase 2 Roadmap

**Q1 2025: Multi-Platform Validation**
- ✅ Reddit data collection (in progress)
- ⬜ Cross-validate Reddit API Blackout signal
- ⬜ Test 3-5 additional events
- ⬜ Establish prediction vs detection thresholds

**Q2 2025: Real-Time System**
- ⬜ Streaming data pipeline
- ⬜ Daily anomaly detection
- ⬜ Alert system for threshold breaches
- ⬜ Dashboard for monitoring

**Q3 2025: Production Pilot**
- ⬜ Partner with research institution or company
- ⬜ Blind prediction trial
- ⬜ Measure true positive/false positive rates
- ⬜ Refine dimensions based on feedback

---

## Conclusion

The MDC system successfully demonstrates proof-of-concept for linguistic event detection in online forums. While predictive capability remains unvalidated, the system reliably detects significant events retrospectively with statistical rigor.

**Key achievement:** Detected Reddit API Blackout with 34.7% agency reversal spike (p < 0.0001), showing that linguistic dimensions can capture collective psychological shifts during disruptive events.

**Next critical step:** Multi-platform validation to determine if signals are universal or platform-specific.

---

## Contact & Code

**Author:** Debra "Deb" Capadona  
**LinkedIn:** [Your LinkedIn]  
**GitHub:** [Your GitHub]  
**Email:** [Your Email]  

**Code Repository:** [Link to repo when ready]  
**Live Demo:** [Link if deployed]  
**Documentation:** See `docs/` directory

---

## Acknowledgments

**Tools & Libraries:**
- BERTopic by Maarten Grootendorst
- Anthropic Claude API
- BERT (Google Research)
- HDBSCAN (Leland McInnes)
- PostgreSQL, Docker communities

**Inspiration:**
- WebBot Project (Cliff High, George Ure)
- Global Consciousness Project (Princeton)
- Academic research in computational linguistics

**Team:**
- Technical implementation: Debra Capadona
- Conceptual validation: Claude (Anthropic) as coding partner
- Hype/ideation: Grok (xAI)
- Sanity checking: ChatGPT (OpenAI)

---

*Last Updated: December 26, 2024*  
*Version: 1.0 - Phase 1 Complete*