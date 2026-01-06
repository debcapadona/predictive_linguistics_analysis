# Edge Condition Analysis  
**Building a Production System with AI as Co-Pilot**

## Executive Summary

This repository documents the design and delivery of a production-grade NLP analysis platform built in six weeks using AI-assisted development.

Implementation code was AI-generated from explicitly defined requirements using structured prompt engineering, while architectural decisions, validation methods, and delivery accountability remained fully human-owned.

The project demonstrates how experienced technical leadership can responsibly use AI to accelerate implementation while maintaining rigor, scalability, and correctness.

---

## What Was Built

- Ingested and processed **197,496 Hacker News stories (2024–2025)**
- Trained **9 custom BERT classifiers** to score abstract linguistic dimensions
- Implemented a **topic classification pipeline using the Groq API** for fast LLM-based inference
- Stored **5.2M+ word-level associations** in PostgreSQL
- Implemented **GPU-accelerated inference** (~300 stories/second for BERT models)
- Established statistical baselines and event-level validation
- Delivered interactive visualizations and a portfolio site
- Built a reproducible local environment using Docker and database migrations

---

## How AI Was Used

AI was used as an implementation accelerator and analytical assistant, with clearly defined roles and boundaries. All final decisions, validation, and accountability remained human-owned.

### Human Responsibilities

- System architecture and data flow design  
- Definition of requirements and success criteria  
- Statistical methodology and validation choices  
- Schema design and performance considerations  
- Identification and correction of errors  
- Decisions to refactor or rebuild components  
- Final acceptance of all outputs  

### AI Systems and Roles

- **Claude (Anthropic):** Implementation, coding, debugging  
- **Grok (xAI):** Creative thinking, concept expansion, brainstorming  
- **ChatGPT:** Sanity checking, governance, validation  
- **Groq API:** High-throughput LLM inference for topic classification within the processing pipeline  

This separation of responsibilities enabled rapid iteration while preserving engineering discipline and analytical rigor.

---

## System Overview

### Data Pipeline

- Incremental ingestion from the Hacker News API  
- Checkpointing, rate limiting, and crash recovery  
- Long-running execution with no data loss  

### Machine Learning

- 9 custom BERT-based classifiers trained with limited labeled data  
- Weak supervision and iterative refinement  
- Hyperparameters tuned based on observed metrics  
- GPU acceleration using CUDA for batch inference  

### Topic Classification (Groq API)

- LLM-based topic classification integrated into the pipeline  
- Optimized for low-latency, high-throughput inference using the Groq API  
- Used to support downstream aggregation and analysis rather than as a primary modeling dependency  

### Database & Scale

- PostgreSQL 16  
- Schema designed for millions of word-level records  
- Indexed for time-range, word-level, and dimensional queries  
- Alembic migrations for controlled schema evolution  

### Statistical Validation

- Median-based baselines for skewed distributions  
- Hypothesis testing with effect sizes  
- Event-level analysis using known external events  
- Cross-dimensional coherence analysis  

### Visualization

- Interactive Plotly dashboards  
- Dimension toggling, zoom, and temporal navigation  
- Portfolio site focused on exploration rather than explanation  

---

## Prompt Engineering Approach

Effective use of AI relied on disciplined prompting patterns:

- Clear requirements before requesting code  
- Explicit discussion of scale and failure modes  
- Requests for complete artifacts rather than snippets  
- Iterative validation and correction  
- Trade-off analysis instead of one-shot answers  

Different AI systems were used intentionally for different tasks, with the human maintaining overall system coherence and quality control.

---

## Constraints and Limitations

- AI outputs varied in quality and consistency  
- Context limits required task decomposition  
- Some suggested approaches were incorrect or impractical  

These issues were addressed through testing, review, version control, and rebuild decisions when appropriate.

---

## Why This Repository Is Public

This repository is intended as a transparent example of:

- End-to-end system delivery  
- Responsible AI-assisted development  
- Technical leadership across data engineering, machine learning, infrastructure, and analysis  

It is not presented as a reusable library or finished product.

---

## Live Links

- **Portfolio:** https://edgeconditionanalysis.com  
- **Repository:** https://github.com/debcapadona/predictive_linguistics_analysis  
- **LinkedIn:** https://www.linkedin.com/in/debcapadona/

---

## Contact

**Debra Capadona**  
Senior Technical Program Manager  
deb.capadona@gmail.com
