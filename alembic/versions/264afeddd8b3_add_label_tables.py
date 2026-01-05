"""add_label_tables

Revision ID: 264afeddd8b3
Revises: 
Create Date: 2024-12-23

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '264afeddd8b3'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Topic taxonomy - 3-tier hierarchy
    op.execute("""
        CREATE TABLE topic_taxonomy (
            id SERIAL PRIMARY KEY,
            topic_name VARCHAR(100) NOT NULL UNIQUE,
            tier INTEGER NOT NULL CHECK (tier IN (1, 2, 3)),
            parent_id INTEGER REFERENCES topic_taxonomy(id),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    # Comment/story labels
    op.execute("""
        CREATE TABLE comment_labels (
            id SERIAL PRIMARY KEY,
            comment_id INTEGER NOT NULL,
            label_type VARCHAR(50) NOT NULL,
            label_value VARCHAR(50),
            topic_id INTEGER REFERENCES topic_taxonomy(id),
            confidence FLOAT,
            source VARCHAR(20) NOT NULL,
            labeled_at TIMESTAMP DEFAULT NOW(),
            labeled_by VARCHAR(100),
            CHECK (
                (label_type = 'topic' AND topic_id IS NOT NULL AND label_value IS NULL) OR
                (label_type != 'topic' AND topic_id IS NULL AND label_value IS NOT NULL)
            )
        )
    """)
    
    # Word-level labels
    op.execute("""
        CREATE TABLE word_labels (
            id SERIAL PRIMARY KEY,
            word VARCHAR(100) NOT NULL,
            label_type VARCHAR(50) NOT NULL,
            label_value VARCHAR(50) NOT NULL,
            confidence FLOAT,
            source VARCHAR(20) NOT NULL,
            labeled_at TIMESTAMP DEFAULT NOW(),
            labeled_by VARCHAR(100),
            notes TEXT,
            UNIQUE(word, label_type, label_value)
        )
    """)
    
    # Indexes for performance
    op.execute("CREATE INDEX idx_comment_labels_comment_id ON comment_labels(comment_id)")
    op.execute("CREATE INDEX idx_comment_labels_type_value ON comment_labels(label_type, label_value)")
    op.execute("CREATE INDEX idx_comment_labels_topic_id ON comment_labels(topic_id)")
    op.execute("CREATE INDEX idx_word_labels_word ON word_labels(word)")
    op.execute("CREATE INDEX idx_word_labels_type_value ON word_labels(label_type, label_value)")
    op.execute("CREATE INDEX idx_topic_taxonomy_parent ON topic_taxonomy(parent_id)")


def downgrade() -> None:
    # Drop in reverse order (indexes, then tables with dependencies)
    op.execute("DROP INDEX IF EXISTS idx_topic_taxonomy_parent")
    op.execute("DROP INDEX IF EXISTS idx_word_labels_type_value")
    op.execute("DROP INDEX IF EXISTS idx_word_labels_word")
    op.execute("DROP INDEX IF EXISTS idx_comment_labels_topic_id")
    op.execute("DROP INDEX IF EXISTS idx_comment_labels_type_value")
    op.execute("DROP INDEX IF EXISTS idx_comment_labels_comment_id")
    op.execute("DROP TABLE IF EXISTS word_labels")
    op.execute("DROP TABLE IF EXISTS comment_labels")
    op.execute("DROP TABLE IF EXISTS topic_taxonomy")