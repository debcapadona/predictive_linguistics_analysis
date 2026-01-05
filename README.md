# Predictive Linguistics Analysis Platform

Large-scale NLP analysis platform demonstrating technical program management and data engineering capabilities through the analysis of linguistic patterns in online discourse.

## Overview

This system processes 197,496 Hacker News stories (2024-2025) using 9 custom-trained BERT models to analyze subtle linguistic patterns. The platform demonstrates end-to-end project delivery: system architecture design, database optimization, GPU-accelerated ML inference, statistical validation, and production-grade data pipelines.

## Key Features

- **9 BERT Linguistic Dimensions**: emotional_valence_shift, temporal_bleed, certainty_collapse, time_compression, agency_reversal, metaphor_cluster_density, novel_meme_explosion, sacred_profane_ratio, pronoun_flip
- **Interactive Visualizations**: Plotly dashboards for exploratory analysis
- **Statistical Rigor**: Baseline establishment, t-tests, effect sizes, coherence scoring
- **Scalable Architecture**: PostgreSQL, Docker, GPU acceleration
- **Word Burst Analysis**: Topic dominance tracking across 24 months

## Tech Stack

- Python 3.11
- PyTorch & BERT Transformers
- PostgreSQL 16 with Alembic migrations
- Docker containerization
- GPU/CUDA acceleration
- Plotly for interactive visualization
- BERTopic for topic modeling
- NumPy, Pandas, SciPy for statistical analysis

## Project Structure
```
linguistic-predictor/
├── scripts/              # Data collection, processing, and analysis scripts
├── models/              # Trained BERT models
├── visualizations/      # Portfolio site and interactive charts
├── data/                # Processed datasets (not included in repo)
├── migrations/          # Alembic database migrations
├── docker-compose.yml   # PostgreSQL container setup
└── README.md
```

## Performance Metrics

- **Processing Speed**: ~300 stories/second (GPU-accelerated)
- **Data Volume**: 197K stories, 5.2M word-level associations
- **Model Performance**: Best correlation 0.76 (agency_reversal dimension)
- **Topic Discovery**: 67-topic taxonomy with 3-tier hierarchy

## Portfolio Site

Interactive portfolio showcasing the platform's capabilities: [Coming Soon]

## Contact

Debra Capadona  
Senior Technical Program Manager  
[LinkedIn](https://www.linkedin.com/in/debra-capadona/)

---

*This project demonstrates technical program management, large-scale data engineering, ML model deployment, and statistical rigor required for senior technical roles.*