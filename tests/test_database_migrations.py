from __future__ import annotations

from datetime import timedelta

from sqlalchemy import create_engine, text

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
