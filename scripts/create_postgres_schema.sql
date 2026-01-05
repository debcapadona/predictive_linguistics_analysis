-- ============================================================================
-- LINGUISTIC PREDICTOR V2 - PostgreSQL Schema
-- Multi-Dimensional Classification with Word-Level Tagging
-- ============================================================================

-- Core tables (migrated from SQLite)
CREATE TABLE IF NOT EXISTS stories (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT,
    created_at TIMESTAMP NOT NULL,
    content_type TEXT,
    score INTEGER,
    author TEXT,
    num_comments INTEGER
);

CREATE TABLE IF NOT EXISTS comments (
    id TEXT PRIMARY KEY,
    story_id TEXT NOT NULL,
    parent_id TEXT,
    text TEXT,
    author TEXT,
    created_at TIMESTAMP NOT NULL,
    FOREIGN KEY (story_id) REFERENCES stories(id)
);

-- Legacy enrichment tables (preserve existing data)
CREATE TABLE IF NOT EXISTS tension_scores (
    story_id TEXT PRIMARY KEY,
    tension_score REAL,
    release_score REAL,
    net_tension REAL,
    analyzed_at TIMESTAMP,
    FOREIGN KEY (story_id) REFERENCES stories(id)
);

CREATE TABLE IF NOT EXISTS temporal_markers (
    id SERIAL PRIMARY KEY,
    story_id TEXT NOT NULL,
    marker_text TEXT NOT NULL,
    marker_type TEXT,
    position INTEGER,
    extracted_at TIMESTAMP,
    FOREIGN KEY (story_id) REFERENCES stories(id)
);

CREATE TABLE IF NOT EXISTS word_cooccurrences (
    id SERIAL PRIMARY KEY,
    story_id TEXT NOT NULL,
    tension_word TEXT NOT NULL,
    cooccurring_word TEXT NOT NULL,
    distance INTEGER,
    extracted_at TIMESTAMP,
    FOREIGN KEY (story_id) REFERENCES stories(id)
);

CREATE TABLE IF NOT EXISTS story_topics (
    story_id TEXT PRIMARY KEY,
    domain TEXT,
    threat_level TEXT,
    target TEXT,
    domain_confidence REAL,
    threat_confidence REAL,
    target_confidence REAL,
    classified_at TIMESTAMP,
    FOREIGN KEY (story_id) REFERENCES stories(id)
);

CREATE TABLE IF NOT EXISTS named_entities (
    id SERIAL PRIMARY KEY,
    story_id TEXT NOT NULL,
    entity_text TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    start_char INTEGER,
    end_char INTEGER,
    extracted_at TIMESTAMP,
    FOREIGN KEY (story_id) REFERENCES stories(id)
);

-- ============================================================================
-- NEW: MDC (Multi-Dimensional Classification) System
-- ============================================================================

-- Master classification table (stores unique 6D/9D vectors)
CREATE TABLE IF NOT EXISTS mdc_classifications (
    id SERIAL PRIMARY KEY,
    
    -- Dimension 1: Certainty Collapse
    certainty_score REAL,
    certainty_uncertain INTEGER,
    certainty_certain INTEGER,
    
    -- Dimension 2: Pronoun Distribution
    pronoun_first REAL,
    pronoun_third REAL,
    pronoun_collective REAL,
    pronoun_first_count INTEGER,
    pronoun_third_count INTEGER,
    pronoun_collective_count INTEGER,
    
    -- Dimension 3: Emotional Valence
    valence_score REAL,
    valence_positive REAL,
    valence_negative REAL,
    valence_neutral REAL,
    
    -- Dimension 4: Temporal Bleed
    temporal_bleed REAL,
    temporal_detected BOOLEAN,
    temporal_reasoning TEXT,
    
    -- Dimension 5: Time Compression
    time_compression REAL,
    compression_speed_count INTEGER,
    compression_timeline_count INTEGER,
    compression_overwhelm_count INTEGER,
    compression_intensity_count INTEGER,
    
    -- Dimension 6: Sacred/Profane Ratio
    sacred_profane REAL,
    sacred_count INTEGER,
    profane_count INTEGER,
    nihilism_count INTEGER,
    despair_count INTEGER,
    
    -- Dimensions 7-9 (Future expansion)
    novel_meme REAL,
    agency_reversal REAL,
    metaphor_density REAL,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- Ensure uniqueness (deduplication)
    UNIQUE NULLS NOT DISTINCT (
        certainty_score, 
        pronoun_first, 
        pronoun_collective,
        valence_score, 
        temporal_bleed, 
        time_compression, 
        sacred_profane
    )
);

-- Story to classification mapping
CREATE TABLE IF NOT EXISTS story_classifications (
    story_id TEXT PRIMARY KEY,
    classification_id INTEGER NOT NULL REFERENCES mdc_classifications(id),
    classified_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (story_id) REFERENCES stories(id)
);

-- Word tokens with inherited classification
CREATE TABLE IF NOT EXISTS word_tokens (
    id BIGSERIAL PRIMARY KEY,
    story_id TEXT NOT NULL,
    word_text TEXT NOT NULL,
    word_lower TEXT NOT NULL,  -- Lowercase for fast searching
    position INTEGER NOT NULL,
    classification_id INTEGER NOT NULL REFERENCES mdc_classifications(id),
    is_temporal_marker BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (story_id) REFERENCES stories(id)
);

-- ============================================================================
-- Indexes for Performance
-- ============================================================================

-- Story lookups
CREATE INDEX IF NOT EXISTS idx_stories_created_at ON stories(created_at);
CREATE INDEX IF NOT EXISTS idx_stories_content_type ON stories(content_type);

-- Comment lookups
CREATE INDEX IF NOT EXISTS idx_comments_story_id ON comments(story_id);
CREATE INDEX IF NOT EXISTS idx_comments_created_at ON comments(created_at);

-- Topic classification lookups
CREATE INDEX IF NOT EXISTS idx_topics_domain ON story_topics(domain);
CREATE INDEX IF NOT EXISTS idx_topics_threat ON story_topics(threat_level);
CREATE INDEX IF NOT EXISTS idx_topics_target ON story_topics(target);

-- NER lookups
CREATE INDEX IF NOT EXISTS idx_entities_story_id ON named_entities(story_id);
CREATE INDEX IF NOT EXISTS idx_entities_type ON named_entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_text ON named_entities(entity_text);

-- Temporal marker lookups
CREATE INDEX IF NOT EXISTS idx_temporal_story_id ON temporal_markers(story_id);
CREATE INDEX IF NOT EXISTS idx_temporal_marker_text ON temporal_markers(marker_text);

-- Word token lookups (CRITICAL for performance)
CREATE INDEX IF NOT EXISTS idx_word_tokens_story ON word_tokens(story_id);
CREATE INDEX IF NOT EXISTS idx_word_tokens_text ON word_tokens(word_text);
CREATE INDEX IF NOT EXISTS idx_word_tokens_lower ON word_tokens(word_lower);
CREATE INDEX IF NOT EXISTS idx_word_tokens_classification ON word_tokens(classification_id);
CREATE INDEX IF NOT EXISTS idx_word_tokens_temporal ON word_tokens(is_temporal_marker) WHERE is_temporal_marker = TRUE;

-- Classification lookups
CREATE INDEX IF NOT EXISTS idx_classifications_certainty ON mdc_classifications(certainty_score);
CREATE INDEX IF NOT EXISTS idx_classifications_valence ON mdc_classifications(valence_score);
CREATE INDEX IF NOT EXISTS idx_classifications_temporal ON mdc_classifications(temporal_bleed);
CREATE INDEX IF NOT EXISTS idx_classifications_compression ON mdc_classifications(time_compression);
CREATE INDEX IF NOT EXISTS idx_classifications_sacred ON mdc_classifications(sacred_profane);

-- ============================================================================
-- Materialized Views for Fast Queries
-- ============================================================================

-- Word frequency by MDC dimension
CREATE MATERIALIZED VIEW IF NOT EXISTS word_frequency_by_dimension AS
SELECT 
    wt.word_lower,
    COUNT(*) as frequency,
    AVG(mc.certainty_score) as avg_certainty,
    AVG(mc.valence_score) as avg_valence,
    AVG(mc.temporal_bleed) as avg_temporal_bleed,
    AVG(mc.time_compression) as avg_time_compression,
    AVG(mc.sacred_profane) as avg_sacred_profane
FROM word_tokens wt
JOIN mdc_classifications mc ON wt.classification_id = mc.id
GROUP BY wt.word_lower;

-- Refresh command (run after bulk inserts):
-- REFRESH MATERIALIZED VIEW word_frequency_by_dimension;

-- ============================================================================
-- Helper Functions
-- ============================================================================

-- Function to get or create classification
CREATE OR REPLACE FUNCTION get_or_create_classification(
    p_certainty REAL,
    p_pronoun_first REAL,
    p_pronoun_collective REAL,
    p_valence REAL,
    p_temporal_bleed REAL,
    p_time_compression REAL,
    p_sacred_profane REAL
) RETURNS INTEGER AS $$
DECLARE
    v_classification_id INTEGER;
BEGIN
    -- Try to find existing
    SELECT id INTO v_classification_id
    FROM mdc_classifications
    WHERE certainty_score = p_certainty
      AND pronoun_first = p_pronoun_first
      AND pronoun_collective = p_pronoun_collective
      AND valence_score = p_valence
      AND temporal_bleed = p_temporal_bleed
      AND time_compression = p_time_compression
      AND sacred_profane = p_sacred_profane;
    
    -- If not found, create new
    IF v_classification_id IS NULL THEN
        INSERT INTO mdc_classifications (
            certainty_score, pronoun_first, pronoun_collective,
            valence_score, temporal_bleed, time_compression, sacred_profane
        ) VALUES (
            p_certainty, p_pronoun_first, p_pronoun_collective,
            p_valence, p_temporal_bleed, p_time_compression, p_sacred_profane
        ) RETURNING id INTO v_classification_id;
    END IF;
    
    RETURN v_classification_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Success Message
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '✓ Schema created successfully!';
    RAISE NOTICE '  Database: linguistic_predictor_v2';
    RAISE NOTICE '  Tables: 13 core + MDC tables';
    RAISE NOTICE '  Indexes: Optimized for word-level queries';
    RAISE NOTICE '  Ready for migration from SQLite';
END $$;