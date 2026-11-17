import psycopg
from psycopg.rows import dict_row

from . import config


def primary(row_factory=None):
    """Connection to the always-on source database (autocommit)."""
    return psycopg.connect(
        config.PRIMARY_DSN, autocommit=True, connect_timeout=5, row_factory=row_factory
    )


def primary_transaction(row_factory=None):
    """Connection whose ``with`` block is one transaction.

    Used for ingestion: observation inserts, primary-table writes and the
    watermark advance must commit or roll back together.
    """
    return psycopg.connect(
        config.PRIMARY_DSN, autocommit=False, connect_timeout=5, row_factory=row_factory
    )


def replica():
    """Read-only connection to the currently recovered replica."""
    return psycopg.connect(
        config.REPLICA_DSN, autocommit=True, connect_timeout=3, row_factory=dict_row
    )
