from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from app.db.models import Base


def make_engine(database_url: str) -> Engine:
    connect_args = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    return create_engine(database_url, connect_args=connect_args)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def init_db(engine: Engine) -> None:
    Base.metadata.create_all(bind=engine)
    _apply_sqlite_compat_migrations(engine)


def _apply_sqlite_compat_migrations(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    with engine.begin() as conn:
        if "price_snapshots" in tables:
            columns = {column["name"] for column in inspector.get_columns("price_snapshots")}
            if "quality_status" not in columns:
                conn.execute(text("ALTER TABLE price_snapshots ADD COLUMN quality_status VARCHAR(16) NOT NULL DEFAULT 'VALID'"))
            if "quality_reason" not in columns:
                conn.execute(text("ALTER TABLE price_snapshots ADD COLUMN quality_reason VARCHAR(64)"))
            if "source_provider" not in columns:
                conn.execute(text("ALTER TABLE price_snapshots ADD COLUMN source_provider VARCHAR(32)"))
            if "source_endpoint" not in columns:
                conn.execute(text("ALTER TABLE price_snapshots ADD COLUMN source_endpoint VARCHAR(128)"))
            if "source_field" not in columns:
                conn.execute(text("ALTER TABLE price_snapshots ADD COLUMN source_field VARCHAR(64)"))
        if "top_holder_snapshots" in tables:
            columns = {column["name"] for column in inspector.get_columns("top_holder_snapshots")}
            if "dropped_out_top20" not in columns:
                conn.execute(text("ALTER TABLE top_holder_snapshots ADD COLUMN dropped_out_top20 BOOLEAN NOT NULL DEFAULT 0"))
        if "top_holder_peaks" in tables:
            columns = {column["name"] for column in inspector.get_columns("top_holder_peaks")}
            if "watch_started_at" not in columns or _top_holder_peaks_needs_rebuild(conn):
                _rebuild_top_holder_peaks(conn, "watch_started_at" in columns)
        if "social_discovery_cursors" in tables:
            columns = {column["name"] for column in inspector.get_columns("social_discovery_cursors")}
            if "consecutive_no_new_author_runs" not in columns:
                conn.execute(
                    text(
                        "ALTER TABLE social_discovery_cursors "
                        "ADD COLUMN consecutive_no_new_author_runs INTEGER NOT NULL DEFAULT 0"
                    )
                )
        if "social_memories" in tables:
            if _social_memories_needs_rebuild(conn):
                _rebuild_social_memories(conn)
        else:
            conn.execute(
                text(
                    """
                    CREATE TABLE social_memories (
                        id INTEGER NOT NULL PRIMARY KEY,
                        chain VARCHAR(32) NOT NULL,
                        token_address VARCHAR(64) NOT NULL,
                        symbol VARCHAR(64),
                        project_identity VARCHAR(128),
                        project_key VARCHAR(256),
                        source_author_id VARCHAR(128),
                        source_username VARCHAR(128),
                        source_author_type VARCHAR(32) NOT NULL,
                        provider VARCHAR(32) NOT NULL,
                        tweet_id VARCHAR(128) NOT NULL,
                        tweet_url VARCHAR(512),
                        event_time DATETIME NOT NULL,
                        category VARCHAR(64) NOT NULL,
                        summary TEXT NOT NULL,
                        significance VARCHAR(16) NOT NULL,
                        confidence VARCHAR(16) NOT NULL,
                        information_scope VARCHAR(32) NOT NULL,
                        raw_reference_hash VARCHAR(64),
                        triage_version VARCHAR(32) NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(provider, tweet_id, chain, token_address)
                    )
                    """
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX ix_social_memories_token_event_time "
                    "ON social_memories (chain, token_address, event_time)"
                )
            )
            conn.execute(text("CREATE INDEX ix_social_memories_category ON social_memories (category)"))


def _top_holder_peaks_needs_rebuild(conn) -> bool:
    unique_columns: list[list[str]] = []
    for row in conn.exec_driver_sql("PRAGMA index_list('top_holder_peaks')").mappings():
        if not row.get("unique"):
            continue
        index_name = row["name"]
        columns = [
            index_row["name"]
            for index_row in conn.exec_driver_sql(f"PRAGMA index_info('{index_name}')").mappings()
        ]
        unique_columns.append(columns)
    old_unique = ["wallet_id", "token_address", "holder_address"]
    new_unique = ["wallet_id", "token_address", "holder_address", "watch_started_at"]
    return old_unique in unique_columns or new_unique not in unique_columns


def _social_memories_needs_rebuild(conn) -> bool:
    unique_columns: list[list[str]] = []
    for row in conn.exec_driver_sql("PRAGMA index_list('social_memories')").mappings():
        if not row.get("unique"):
            continue
        index_name = row["name"]
        columns = [
            index_row["name"]
            for index_row in conn.exec_driver_sql(f"PRAGMA index_info('{index_name}')").mappings()
        ]
        unique_columns.append(columns)
    old_unique = ["provider", "tweet_id", "token_address"]
    new_unique = ["provider", "tweet_id", "chain", "token_address"]
    return old_unique in unique_columns or new_unique not in unique_columns


def _rebuild_social_memories(conn) -> None:
    conn.execute(text("ALTER TABLE social_memories RENAME TO social_memories_old"))
    conn.execute(
        text(
            """
            CREATE TABLE social_memories (
                id INTEGER NOT NULL PRIMARY KEY,
                chain VARCHAR(32) NOT NULL,
                token_address VARCHAR(64) NOT NULL,
                symbol VARCHAR(64),
                project_identity VARCHAR(128),
                project_key VARCHAR(256),
                source_author_id VARCHAR(128),
                source_username VARCHAR(128),
                source_author_type VARCHAR(32) NOT NULL,
                provider VARCHAR(32) NOT NULL,
                tweet_id VARCHAR(128) NOT NULL,
                tweet_url VARCHAR(512),
                event_time DATETIME NOT NULL,
                category VARCHAR(64) NOT NULL,
                summary TEXT NOT NULL,
                significance VARCHAR(16) NOT NULL,
                confidence VARCHAR(16) NOT NULL,
                information_scope VARCHAR(32) NOT NULL,
                raw_reference_hash VARCHAR(64),
                triage_version VARCHAR(32) NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(provider, tweet_id, chain, token_address)
            )
            """
        )
    )
    conn.execute(
        text(
            """
            INSERT INTO social_memories (
                id,
                chain,
                token_address,
                symbol,
                project_identity,
                project_key,
                source_author_id,
                source_username,
                source_author_type,
                provider,
                tweet_id,
                tweet_url,
                event_time,
                category,
                summary,
                significance,
                confidence,
                information_scope,
                raw_reference_hash,
                triage_version,
                created_at
            )
            SELECT
                id,
                chain,
                token_address,
                symbol,
                project_identity,
                project_key,
                source_author_id,
                source_username,
                source_author_type,
                provider,
                tweet_id,
                tweet_url,
                event_time,
                category,
                summary,
                significance,
                confidence,
                information_scope,
                raw_reference_hash,
                triage_version,
                created_at
            FROM social_memories_old
            """
        )
    )
    conn.execute(text("DROP TABLE social_memories_old"))
    conn.execute(
        text(
            "CREATE INDEX ix_social_memories_token_event_time "
            "ON social_memories (chain, token_address, event_time)"
        )
    )
    conn.execute(text("CREATE INDEX ix_social_memories_category ON social_memories (category)"))


def _rebuild_top_holder_peaks(conn, had_watch_started_at: bool) -> None:
    conn.execute(text("ALTER TABLE top_holder_peaks RENAME TO top_holder_peaks_old"))
    conn.execute(
        text(
            """
            CREATE TABLE top_holder_peaks (
                id INTEGER NOT NULL PRIMARY KEY,
                wallet_id INTEGER NOT NULL,
                token_address VARCHAR(64) NOT NULL,
                holder_address VARCHAR(64) NOT NULL,
                watch_started_at DATETIME NOT NULL,
                peak_balance NUMERIC(38, 18),
                peak_hold_percentage NUMERIC(18, 10),
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(wallet_id) REFERENCES wallets (id),
                UNIQUE(wallet_id, token_address, holder_address, watch_started_at)
            )
            """
        )
    )
    if had_watch_started_at:
        conn.execute(
            text(
                """
                INSERT INTO top_holder_peaks (
                    id,
                    wallet_id,
                    token_address,
                    holder_address,
                    watch_started_at,
                    peak_balance,
                    peak_hold_percentage,
                    updated_at
                )
                SELECT
                    id,
                    wallet_id,
                    token_address,
                    holder_address,
                    watch_started_at,
                    peak_balance,
                    peak_hold_percentage,
                    updated_at
                FROM top_holder_peaks_old
                WHERE watch_started_at IS NOT NULL
                """
            )
        )
    conn.execute(text("DROP TABLE top_holder_peaks_old"))
    conn.execute(text("CREATE INDEX ix_top_holder_peaks_wallet_token ON top_holder_peaks (wallet_id, token_address)"))


@contextmanager
def session_scope(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
