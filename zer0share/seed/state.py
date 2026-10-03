import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from .models import FINANCIAL_TABLES, FinancialSeedManifest, FinancialSeedTask, SeedTaskStatus


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class FinancialSeedState:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS financial_seed_runs (
                run_id TEXT PRIMARY KEY,
                manifest_path TEXT NOT NULL,
                manifest_hash TEXT NOT NULL,
                ticker_count INTEGER NOT NULL,
                tables_csv TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS financial_seed_tasks (
                run_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                ticker TEXT NOT NULL,
                status TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                started_at TEXT,
                finished_at TEXT,
                rows_fetched INTEGER,
                rows_written INTEGER,
                error_type TEXT,
                error_message TEXT,
                last_updated_at TEXT NOT NULL,
                PRIMARY KEY (run_id, table_name, ticker),
                FOREIGN KEY (run_id) REFERENCES financial_seed_runs(run_id)
            )
        """)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
        return False

    def create_run(
        self,
        manifest: FinancialSeedManifest,
        tables: tuple[str, ...],
        start_date: str,
        end_date: str,
    ) -> str:
        if not tables or set(tables) - set(FINANCIAL_TABLES):
            raise ValueError("financial seed tables are invalid")
        run_id = uuid4().hex
        now = _now()
        with self._conn:
            self._conn.execute(
                """INSERT INTO financial_seed_runs
                (run_id, manifest_path, manifest_hash, ticker_count, tables_csv, start_date, end_date, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, manifest.path, manifest.sha256, len(manifest.tickers), ",".join(tables), start_date, end_date, now),
            )
            self._conn.executemany(
                """INSERT INTO financial_seed_tasks
                (run_id, table_name, ticker, status, start_date, end_date, last_updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                [
                    (run_id, table, ticker, SeedTaskStatus.PENDING, start_date, end_date, now)
                    for table in tables
                    for ticker in manifest.tickers
                ],
            )
        return run_id

    def run_metadata(self, run_id: str) -> dict[str, object]:
        row = self._conn.execute(
            "SELECT * FROM financial_seed_runs WHERE run_id = ?", [run_id]
        ).fetchone()
        if row is None:
            raise ValueError(f"financial seed run not found: {run_id}")
        return dict(row)

    def verify_manifest(self, run_id: str, manifest: FinancialSeedManifest) -> None:
        metadata = self.run_metadata(run_id)
        if metadata["manifest_hash"] != manifest.sha256:
            raise ValueError("financial seed manifest hash does not match run")

    def reset_running(self, run_id: str) -> int:
        with self._conn:
            cursor = self._conn.execute(
                """UPDATE financial_seed_tasks
                SET status = ?, last_updated_at = ?
                WHERE run_id = ? AND status = ?""",
                (SeedTaskStatus.PENDING, _now(), run_id, SeedTaskStatus.RUNNING),
            )
        return cursor.rowcount

    def eligible_tasks(
        self,
        run_id: str,
        table_name: str | None = None,
        limit: int | None = None,
        retry_failed: bool = False,
    ) -> list[FinancialSeedTask]:
        statuses = [SeedTaskStatus.PENDING]
        if retry_failed:
            statuses.append(SeedTaskStatus.FAILED)
        clauses = ["run_id = ?", f"status IN ({','.join('?' for _ in statuses)})"]
        params: list[object] = [run_id, *statuses]
        if table_name is not None:
            clauses.append("table_name = ?")
            params.append(table_name)
        sql = "SELECT * FROM financial_seed_tasks WHERE " + " AND ".join(clauses) + " ORDER BY table_name, ticker"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        rows = self._conn.execute(sql, params).fetchall()
        return [
            FinancialSeedTask(
                row["run_id"], row["table_name"], row["ticker"],
                SeedTaskStatus(row["status"]), row["start_date"], row["end_date"], row["attempt_count"],
            )
            for row in rows
        ]

    def mark_running(self, task: FinancialSeedTask) -> None:
        with self._conn:
            self._conn.execute(
                """UPDATE financial_seed_tasks
                SET status = ?, attempt_count = attempt_count + 1, started_at = ?,
                    finished_at = NULL, error_type = NULL, error_message = NULL, last_updated_at = ?
                WHERE run_id = ? AND table_name = ? AND ticker = ?""",
                (SeedTaskStatus.RUNNING, _now(), _now(), task.run_id, task.table_name, task.ticker),
            )

    def finish(
        self,
        task: FinancialSeedTask,
        status: SeedTaskStatus,
        rows_fetched: int | None = None,
        rows_written: int | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        if status not in {SeedTaskStatus.SUCCESS, SeedTaskStatus.VALID_EMPTY, SeedTaskStatus.FAILED}:
            raise ValueError("financial seed task must finish in a terminal state")
        with self._conn:
            self._conn.execute(
                """UPDATE financial_seed_tasks
                SET status = ?, finished_at = ?, rows_fetched = ?, rows_written = ?,
                    error_type = ?, error_message = ?, last_updated_at = ?
                WHERE run_id = ? AND table_name = ? AND ticker = ?""",
                (status, _now(), rows_fetched, rows_written, error_type, error_message, _now(), task.run_id, task.table_name, task.ticker),
            )

    def summary(self, run_id: str) -> dict[str, object]:
        self.run_metadata(run_id)
        rows = self._conn.execute(
            """SELECT table_name, status, COUNT(*) AS count
            FROM financial_seed_tasks WHERE run_id = ?
            GROUP BY table_name, status ORDER BY table_name, status""", [run_id]
        ).fetchall()
        total = {status.value: 0 for status in SeedTaskStatus}
        by_table: dict[str, dict[str, int]] = {}
        for row in rows:
            by_table.setdefault(row["table_name"], {status.value: 0 for status in SeedTaskStatus})
            by_table[row["table_name"]][row["status"]] = row["count"]
            total[row["status"]] += row["count"]
        return {"total": total, "by_table": by_table}

    def failed_tasks(self, run_id: str) -> list[dict[str, object]]:
        rows = self._conn.execute(
            """SELECT table_name, ticker, error_type, error_message
            FROM financial_seed_tasks
            WHERE run_id = ? AND status = ? ORDER BY table_name, ticker""",
            [run_id, SeedTaskStatus.FAILED],
        ).fetchall()
        return [dict(row) for row in rows]
