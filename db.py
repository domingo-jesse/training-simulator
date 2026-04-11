import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from logger import get_logger

DB_PATH = Path(__file__).resolve().parent / "trainer.db"
db_logger = get_logger(module="db")


@contextmanager
def get_conn():
    db_logger.debug("Opening database connection.")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
        db_logger.debug("Database transaction committed.")
    except Exception:
        db_logger.exception("Database transaction failed and will be rolled back.")
        conn.rollback()
        raise
    finally:
        conn.close()
        db_logger.debug("Database connection closed.")


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r["name"] for r in rows}


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    if column not in _table_columns(conn, table):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        except sqlite3.OperationalError as exc:
            # SQLite cannot add columns with non-constant defaults (e.g. CURRENT_TIMESTAMP)
            # via ALTER TABLE. Fall back to adding the column without a default, then backfill.
            if "non-constant default" not in str(exc).lower():
                raise

            base_definition = definition.split("DEFAULT", 1)[0].strip()
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {base_definition}")

            if "CURRENT_TIMESTAMP" in definition.upper():
                conn.execute(f"UPDATE {table} SET {column} = CURRENT_TIMESTAMP WHERE {column} IS NULL")


def init_db() -> None:
    db_logger.info("Initializing database schema.", db_path=str(DB_PATH))
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS organizations (
                organization_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                email TEXT,
                google_subject TEXT,
                role TEXT NOT NULL,
                team TEXT,
                organization_id INTEGER,
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(organization_id) REFERENCES organizations(organization_id)
            );

            CREATE TABLE IF NOT EXISTS modules (
                module_id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                category TEXT NOT NULL,
                difficulty TEXT NOT NULL,
                description TEXT,
                estimated_time TEXT,
                scenario_ticket TEXT,
                scenario_context TEXT,
                hidden_root_cause TEXT,
                expected_reasoning_path TEXT,
                expected_diagnosis TEXT,
                expected_next_steps TEXT,
                expected_customer_response TEXT,
                lesson_takeaway TEXT,
                organization_id INTEGER,
                status TEXT DEFAULT 'published',
                learning_objectives TEXT,
                content_sections TEXT,
                completion_requirements TEXT,
                quiz_required INTEGER DEFAULT 0,
                created_by INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(organization_id) REFERENCES organizations(organization_id),
                FOREIGN KEY(created_by) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS investigation_actions (
                action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                module_id INTEGER NOT NULL,
                action_name TEXT NOT NULL,
                revealed_information TEXT NOT NULL,
                FOREIGN KEY(module_id) REFERENCES modules(module_id)
            );

            CREATE TABLE IF NOT EXISTS attempts (
                attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                module_id INTEGER NOT NULL,
                organization_id INTEGER,
                diagnosis_answer TEXT,
                next_steps_answer TEXT,
                customer_response TEXT,
                escalation_choice TEXT,
                notes TEXT,
                understanding_score REAL,
                investigation_score REAL,
                solution_score REAL,
                communication_score REAL,
                total_score REAL,
                ai_feedback TEXT,
                strengths TEXT,
                missed_points TEXT,
                best_practice_reasoning TEXT,
                recommended_response TEXT,
                takeaway_summary TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(user_id),
                FOREIGN KEY(module_id) REFERENCES modules(module_id),
                FOREIGN KEY(organization_id) REFERENCES organizations(organization_id)
            );

            CREATE TABLE IF NOT EXISTS action_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                attempt_id INTEGER NOT NULL,
                action_name TEXT NOT NULL,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id)
            );

            CREATE TABLE IF NOT EXISTS assignments (
                assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                organization_id INTEGER NOT NULL,
                module_id INTEGER NOT NULL,
                learner_id INTEGER NOT NULL,
                assigned_by INTEGER,
                due_date TEXT,
                assigned_at TEXT DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                FOREIGN KEY(organization_id) REFERENCES organizations(organization_id),
                FOREIGN KEY(module_id) REFERENCES modules(module_id),
                FOREIGN KEY(learner_id) REFERENCES users(user_id),
                FOREIGN KEY(assigned_by) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS learner_profiles (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL UNIQUE,
                full_name TEXT NOT NULL,
                team TEXT,
                status TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'inactive', 'on_leave')),
                last_activity TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS module_assignments (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                module_id TEXT NOT NULL,
                assigned_at TEXT DEFAULT CURRENT_TIMESTAMP,
                assigned_by TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (user_id, module_id),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(module_id) REFERENCES modules(id) ON DELETE CASCADE,
                FOREIGN KEY(assigned_by) REFERENCES users(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS module_progress (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                module_id TEXT NOT NULL,
                progress_percent INTEGER NOT NULL DEFAULT 0
                    CHECK (progress_percent >= 0 AND progress_percent <= 100),
                started_at TEXT,
                completed_at TEXT,
                last_activity_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (user_id, module_id),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(module_id) REFERENCES modules(id) ON DELETE CASCADE
            );
            """
        )

        # Backward-compatible migrations
        _ensure_column(conn, "users", "email", "TEXT")
        _ensure_column(conn, "users", "google_subject", "TEXT")
        _ensure_column(conn, "users", "organization_id", "INTEGER")
        _ensure_column(conn, "users", "is_active", "INTEGER DEFAULT 1")
        _ensure_column(conn, "users", "id", "TEXT")
        _ensure_column(conn, "users", "username", "TEXT")
        _ensure_column(conn, "users", "password_hash", "TEXT")
        _ensure_column(conn, "users", "auth_provider", "TEXT DEFAULT 'local_password'")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_external_id ON users(id)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users(username)")

        _ensure_column(conn, "modules", "organization_id", "INTEGER")
        _ensure_column(conn, "modules", "status", "TEXT DEFAULT 'published'")
        _ensure_column(conn, "modules", "learning_objectives", "TEXT")
        _ensure_column(conn, "modules", "content_sections", "TEXT")
        _ensure_column(conn, "modules", "completion_requirements", "TEXT")
        _ensure_column(conn, "modules", "quiz_required", "INTEGER DEFAULT 0")
        _ensure_column(conn, "modules", "created_by", "INTEGER")
        _ensure_column(conn, "modules", "created_at", "TEXT DEFAULT CURRENT_TIMESTAMP")
        _ensure_column(conn, "modules", "updated_at", "TEXT DEFAULT CURRENT_TIMESTAMP")
        _ensure_column(conn, "modules", "id", "TEXT")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_modules_external_id ON modules(id)")

        _ensure_column(conn, "attempts", "organization_id", "INTEGER")

        conn.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_learner_profiles_user_id ON learner_profiles(user_id);
            CREATE INDEX IF NOT EXISTS idx_learner_profiles_status ON learner_profiles(status);
            CREATE INDEX IF NOT EXISTS idx_module_assignments_user_id ON module_assignments(user_id);
            CREATE INDEX IF NOT EXISTS idx_module_assignments_module_id ON module_assignments(module_id);
            CREATE INDEX IF NOT EXISTS idx_module_progress_user_id ON module_progress(user_id);
            CREATE INDEX IF NOT EXISTS idx_module_progress_module_id ON module_progress(module_id);
            CREATE INDEX IF NOT EXISTS idx_module_progress_completed_at ON module_progress(completed_at);

            DROP TRIGGER IF EXISTS trg_learner_profiles_updated_at;
            CREATE TRIGGER trg_learner_profiles_updated_at
            AFTER UPDATE ON learner_profiles
            FOR EACH ROW
            WHEN NEW.updated_at = OLD.updated_at
            BEGIN
                UPDATE learner_profiles SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
            END;

            DROP TRIGGER IF EXISTS trg_modules_updated_at;
            CREATE TRIGGER trg_modules_updated_at
            AFTER UPDATE ON modules
            FOR EACH ROW
            WHEN NEW.updated_at = OLD.updated_at
            BEGIN
                UPDATE modules SET updated_at = CURRENT_TIMESTAMP WHERE module_id = OLD.module_id;
            END;

            DROP TRIGGER IF EXISTS trg_module_progress_updated_at;
            CREATE TRIGGER trg_module_progress_updated_at
            AFTER UPDATE ON module_progress
            FOR EACH ROW
            WHEN NEW.updated_at = OLD.updated_at
            BEGIN
                UPDATE module_progress SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
            END;

            CREATE VIEW IF NOT EXISTS learner_dashboard_summary AS
            SELECT
                u.id AS user_id,
                lp.full_name AS name,
                lp.team AS team,
                lp.status AS status,
                COUNT(DISTINCT ma.module_id) AS assigned_modules,
                COUNT(DISTINCT CASE WHEN mp.completed_at IS NOT NULL THEN mp.module_id END) AS completed_modules,
                MAX(COALESCE(lp.last_activity, mp.last_activity_at)) AS last_activity
            FROM users u
            JOIN learner_profiles lp
                ON lp.user_id = u.id
            LEFT JOIN module_assignments ma
                ON ma.user_id = u.id
            LEFT JOIN module_progress mp
                ON mp.user_id = u.id
            GROUP BY
                u.id,
                lp.full_name,
                lp.team,
                lp.status,
                lp.last_activity;
            """
        )
    db_logger.info("Database schema initialization complete.")


def fetch_all(query: str, params: Iterable[Any] = ()) -> List[sqlite3.Row]:
    try:
        with get_conn() as conn:
            cur = conn.execute(query, params)
            rows = cur.fetchall()
            db_logger.debug("Database read completed.", operation="fetch_all", row_count=len(rows))
            return rows
    except Exception:
        db_logger.exception("Database read failed.", operation="fetch_all")
        raise


def fetch_one(query: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
    try:
        with get_conn() as conn:
            cur = conn.execute(query, params)
            row = cur.fetchone()
            db_logger.debug("Database read completed.", operation="fetch_one", found=bool(row))
            return row
    except Exception:
        db_logger.exception("Database read failed.", operation="fetch_one")
        raise


def execute(query: str, params: Iterable[Any] = ()) -> int:
    try:
        with get_conn() as conn:
            cur = conn.execute(query, params)
            db_logger.info("Database write completed.", operation="execute", lastrowid=cur.lastrowid)
            return cur.lastrowid
    except Exception:
        db_logger.exception("Database write failed.", operation="execute")
        raise


def executemany(query: str, rows: Iterable[Iterable[Any]]) -> None:
    try:
        buffered_rows = list(rows)
        with get_conn() as conn:
            conn.executemany(query, buffered_rows)
            db_logger.info("Database bulk write completed.", operation="executemany", row_count=len(buffered_rows))
    except Exception:
        db_logger.exception("Database bulk write failed.", operation="executemany")
        raise


def insert_attempt(user_id: int, module_id: int, payload: Dict[str, Any], organization_id: int | None = None) -> int:
    if organization_id is None:
        user = fetch_one("SELECT organization_id FROM users WHERE user_id = ?", (user_id,))
        organization_id = user["organization_id"] if user else None

    return execute(
        """
        INSERT INTO attempts (
            user_id, module_id, organization_id, diagnosis_answer, next_steps_answer, customer_response,
            escalation_choice, notes,
            understanding_score, investigation_score, solution_score, communication_score,
            total_score, ai_feedback, strengths, missed_points,
            best_practice_reasoning, recommended_response, takeaway_summary
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            module_id,
            organization_id,
            payload.get("diagnosis_answer"),
            payload.get("next_steps_answer"),
            payload.get("customer_response"),
            payload.get("escalation_choice"),
            payload.get("notes"),
            payload["category_scores"]["understanding"],
            payload["category_scores"]["investigation"],
            payload["category_scores"]["solution_quality"],
            payload["category_scores"]["communication"],
            payload["total_score"],
            payload["coaching_feedback"],
            json.dumps(payload["strengths"]),
            json.dumps(payload["missed_points"]),
            payload["best_practice_reasoning"],
            payload["recommended_response"],
            payload["takeaway_summary"],
        ),
    )


def log_actions(attempt_id: int, actions: List[str]) -> None:
    if not actions:
        return
    executemany(
        "INSERT INTO action_logs (attempt_id, action_name) VALUES (?, ?)",
        [(attempt_id, action) for action in actions],
    )
