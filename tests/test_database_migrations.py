from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from app.db.database import init_db
from app.utils.time import utc_now


def test_sqlite_rebuilds_old_top_holder_peak_unique_constraint(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path}/legacy.db")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE top_holder_peaks (
                    id INTEGER NOT NULL PRIMARY KEY,
                    wallet_id INTEGER NOT NULL,
                    token_address VARCHAR(64) NOT NULL,
                    holder_address VARCHAR(64) NOT NULL,
                    peak_balance NUMERIC(38, 18),
                    peak_hold_percentage NUMERIC(18, 10),
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(wallet_id, token_address, holder_address)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO top_holder_peaks (
                    wallet_id,
                    token_address,
                    holder_address,
                    peak_balance,
                    peak_hold_percentage
                )
                VALUES (1, '0xtoken', '0xholder', 100, 0.05)
                """
            )
        )

    init_db(engine)

    first_watch = utc_now() - timedelta(days=1)
    second_watch = utc_now()
    with engine.begin() as conn:
        unique_indexes = []
        for row in conn.exec_driver_sql("PRAGMA index_list('top_holder_peaks')").mappings():
            if row.get("unique"):
                columns = [
                    index_row["name"]
                    for index_row in conn.exec_driver_sql(f"PRAGMA index_info('{row['name']}')").mappings()
                ]
                unique_indexes.append(columns)
        assert ["wallet_id", "token_address", "holder_address", "watch_started_at"] in unique_indexes
        assert ["wallet_id", "token_address", "holder_address"] not in unique_indexes

        conn.execute(
            text(
                """
                INSERT INTO top_holder_peaks (
                    wallet_id,
                    token_address,
                    holder_address,
                    watch_started_at,
                    peak_balance,
                    peak_hold_percentage
                )
                VALUES (1, '0xtoken', '0xholder', :watch_started_at, 100, 0.05)
                """
            ),
            {"watch_started_at": first_watch},
        )
        conn.execute(
            text(
                """
                INSERT INTO top_holder_peaks (
                    wallet_id,
                    token_address,
                    holder_address,
                    watch_started_at,
                    peak_balance,
                    peak_hold_percentage
                )
                VALUES (1, '0xtoken', '0xholder', :watch_started_at, 50, 0.02)
                """
            ),
            {"watch_started_at": second_watch},
        )
        count = conn.scalar(
            text(
                """
                SELECT COUNT(*)
                FROM top_holder_peaks
                WHERE wallet_id = 1
                  AND token_address = '0xtoken'
                  AND holder_address = '0xholder'
                """
            )
        )

    assert count == 2


def test_sqlite_adds_price_snapshot_quality_and_source_columns(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path}/legacy_price.db")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE price_snapshots (
                    id INTEGER NOT NULL PRIMARY KEY,
                    wallet_id INTEGER NOT NULL,
                    chain VARCHAR(32) NOT NULL,
                    token_address VARCHAR(64) NOT NULL,
                    symbol VARCHAR(64),
                    price_usd NUMERIC(38, 18) NOT NULL,
                    balance NUMERIC(38, 18) NOT NULL,
                    usd_value NUMERIC(24, 8),
                    observed_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO price_snapshots (
                    wallet_id,
                    chain,
                    token_address,
                    symbol,
                    price_usd,
                    balance,
                    usd_value,
                    observed_at
                )
                VALUES (1, 'robinhood', '0xtoken', 'AI', 0.23, 100, 23, :observed_at)
                """
            ),
            {"observed_at": utc_now()},
        )

    init_db(engine)

    with engine.begin() as conn:
        columns = {
            row["name"]
            for row in conn.exec_driver_sql("PRAGMA table_info('price_snapshots')").mappings()
        }
        row = conn.execute(
            text(
                """
                SELECT quality_status,
                       quality_reason,
                       source_provider,
                       source_endpoint,
                       source_field
                FROM price_snapshots
                WHERE token_address = '0xtoken'
                """
            )
        ).mappings().one()

    assert {
        "quality_status",
        "quality_reason",
        "source_provider",
        "source_endpoint",
        "source_field",
    }.issubset(columns)
    assert row["quality_status"] == "VALID"
    assert row["quality_reason"] is None
    assert row["source_provider"] is None
    assert row["source_endpoint"] is None
    assert row["source_field"] is None


def test_sqlite_creates_social_kol_profiles_table_and_indexes(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path}/social_kol.db")

    init_db(engine)

    with engine.begin() as conn:
        columns = {
            row["name"]
            for row in conn.exec_driver_sql("PRAGMA table_info('social_kol_profiles')").mappings()
        }
        indexes = {
            row["name"]
            for row in conn.exec_driver_sql("PRAGMA index_list('social_kol_profiles')").mappings()
        }

    assert {
        "x_author_id",
        "username",
        "normalized_username",
        "status",
        "follower_count",
        "crypto_relevant",
        "confidence",
        "category",
        "sources_json",
        "verification_method",
        "verification_reason",
        "sample_tweet_ids_json",
        "verified_at",
        "recheck_after",
        "manual_override",
    }.issubset(columns)
    assert {
        "ix_social_kol_profiles_author_id",
        "ix_social_kol_profiles_username",
        "ix_social_kol_profiles_status",
        "ix_social_kol_profiles_recheck_after",
    }.issubset(indexes)


def test_sqlite_creates_social_discovery_cursors_table_and_indexes(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path}/social_discovery.db")

    init_db(engine)

    with engine.begin() as conn:
        columns = {
            row["name"]
            for row in conn.exec_driver_sql("PRAGMA table_info('social_discovery_cursors')").mappings()
        }
        indexes = {
            row["name"]
            for row in conn.exec_driver_sql("PRAGMA index_list('social_discovery_cursors')").mappings()
        }

    assert {
        "watch_state_id",
        "wallet_id",
        "chain",
        "token_address",
        "query_kind",
        "last_attempt_at",
        "last_success_at",
        "latest_seen_posted_at",
        "next_due_at",
        "consecutive_no_new_author_runs",
        "api_calls",
        "error_count",
        "last_error",
    }.issubset(columns)
    assert {
        "ix_social_discovery_cursors_next_due",
        "ix_social_discovery_cursors_wallet_token",
    }.issubset(indexes)


def test_sqlite_creates_social_memories_table_and_indexes(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path}/social_memory.db")

    init_db(engine)

    with engine.begin() as conn:
        columns = {
            row["name"]
            for row in conn.exec_driver_sql("PRAGMA table_info('social_memories')").mappings()
        }
        indexes = {
            row["name"]
            for row in conn.exec_driver_sql("PRAGMA index_list('social_memories')").mappings()
        }
        unique_indexes = []
        for row in conn.exec_driver_sql("PRAGMA index_list('social_memories')").mappings():
            if row.get("unique"):
                unique_indexes.append(
                    [
                        index_row["name"]
                        for index_row in conn.exec_driver_sql(f"PRAGMA index_info('{row['name']}')").mappings()
                    ]
                )

    assert {
        "chain",
        "token_address",
        "symbol",
        "project_identity",
        "project_key",
        "source_author_id",
        "source_username",
        "source_author_type",
        "provider",
        "tweet_id",
        "tweet_url",
        "event_time",
        "category",
        "summary",
        "significance",
        "confidence",
        "information_scope",
        "raw_reference_hash",
        "triage_version",
    }.issubset(columns)
    assert {
        "ix_social_memories_token_event_time",
        "ix_social_memories_category",
    }.issubset(indexes)
    assert ["provider", "tweet_id", "chain", "token_address"] in unique_indexes


def test_sqlite_rebuilds_social_memories_chain_aware_unique_and_preserves_rows(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path}/legacy_social_memory.db")
    with engine.begin() as conn:
        _create_old_social_memories(conn)
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
                    source_author_type,
                    provider,
                    tweet_id,
                    event_time,
                    category,
                    summary,
                    significance,
                    confidence,
                    information_scope,
                    triage_version
                )
                VALUES (
                    1,
                    'robinhood',
                    '0xtoken',
                    'ROBBIE',
                    'ProjectUser',
                    'robinhood:0xtoken',
                    'project_x',
                    'twitterapi_io',
                    'tweet-1',
                    '2026-09-09 01:00:00',
                    'development',
                    'Project announced mainnet launch.',
                    'medium',
                    'high',
                    'project',
                    'social_v2c_project_dev_memory_v1'
                )
                """
            )
        )

    init_db(engine)
    init_db(engine)

    with engine.begin() as conn:
        unique_indexes = _unique_indexes(conn, "social_memories")
        rows = list(conn.exec_driver_sql("SELECT chain, token_address, tweet_id, summary FROM social_memories").mappings())
        indexes = {
            row["name"]
            for row in conn.exec_driver_sql("PRAGMA index_list('social_memories')").mappings()
        }
        conn.execute(
            text(
                """
                INSERT INTO social_memories (
                    chain,
                    token_address,
                    source_author_type,
                    provider,
                    tweet_id,
                    event_time,
                    category,
                    summary,
                    significance,
                    confidence,
                    information_scope,
                    triage_version
                )
                VALUES (
                    'base',
                    '0xtoken',
                    'project_x',
                    'twitterapi_io',
                    'tweet-1',
                    '2026-09-09 01:00:00',
                    'development',
                    'Same tweet on another chain.',
                    'medium',
                    'high',
                    'project',
                    'social_v2c_project_dev_memory_v1'
                )
                """
            )
        )
        with pytest.raises(IntegrityError):
            conn.execute(
                text(
                    """
                    INSERT INTO social_memories (
                        chain,
                        token_address,
                        source_author_type,
                        provider,
                        tweet_id,
                        event_time,
                        category,
                        summary,
                        significance,
                        confidence,
                        information_scope,
                        triage_version
                    )
                    VALUES (
                        'robinhood',
                        '0xtoken',
                        'project_x',
                        'twitterapi_io',
                        'tweet-1',
                        '2026-09-09 01:00:00',
                        'development',
                        'Duplicate same chain.',
                        'medium',
                        'high',
                        'project',
                        'social_v2c_project_dev_memory_v1'
                    )
                    """
                )
            )

    assert ["provider", "tweet_id", "chain", "token_address"] in unique_indexes
    assert ["provider", "tweet_id", "token_address"] not in unique_indexes
    assert {
        "ix_social_memories_token_event_time",
        "ix_social_memories_category",
    }.issubset(indexes)
    assert len(rows) == 1
    assert rows[0]["summary"] == "Project announced mainnet launch."


def _unique_indexes(conn, table_name: str) -> list[list[str]]:  # noqa: ANN001
    unique_indexes = []
    for row in conn.exec_driver_sql(f"PRAGMA index_list('{table_name}')").mappings():
        if row.get("unique"):
            unique_indexes.append(
                [
                    index_row["name"]
                    for index_row in conn.exec_driver_sql(f"PRAGMA index_info('{row['name']}')").mappings()
                ]
            )
    return unique_indexes


def _create_old_social_memories(conn) -> None:  # noqa: ANN001
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
                UNIQUE(provider, tweet_id, token_address)
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
