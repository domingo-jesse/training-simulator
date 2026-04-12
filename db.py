import json
import os
import re
import tomllib
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

from logger import get_logger

DB_PATH = Path(__file__).resolve().parent / "trainer.db"  # legacy, unused in Supabase mode

def _load_database_url() -> str:
    """Resolve Postgres URL from environment and optional Streamlit secrets."""
    url_keys = ("DATABASE_URL", "SUPABASE_DB_URL", "SUPABASE_DATABASE_URL", "POSTGRES_URL")

    for key in url_keys:
        value = os.getenv(key, "").strip()
        if value:
            return value

    secrets_path = Path(__file__).resolve().parent / ".streamlit" / "secrets.toml"
    if not secrets_path.exists():
        return ""

    try:
        secrets = tomllib.loads(secrets_path.read_text())
    except Exception:
        return ""

    for key in url_keys:
        value = str(secrets.get(key, "")).strip()
        if value:
            return value

    return ""


DATABASE_URL = _load_database_url()


def _ensure_postgres_database_url(url: str) -> None:
    if not url:
        raise RuntimeError(
            "DATABASE_URL is required. Configure Supabase/Postgres via environment variable "
            "or .streamlit/secrets.toml."
        )
    if not (url.startswith("postgres://") or url.startswith("postgresql://")):
        raise RuntimeError("DATABASE_URL must use postgres:// or postgresql:// for Supabase.")


_ensure_postgres_database_url(DATABASE_URL)
USE_POSTGRES = True
RUNTIME_USE_POSTGRES = True
db_logger = get_logger(module="db")


def _infer_target_table(query: str) -> str | None:
    compact = " ".join((query or "").split())
    patterns = (
        r"^\s*INSERT\s+INTO\s+([a-zA-Z0-9_.\"]+)",
        r"^\s*UPDATE\s+([a-zA-Z0-9_.\"]+)",
        r"^\s*DELETE\s+FROM\s+([a-zA-Z0-9_.\"]+)",
    )
    for pattern in patterns:
        match = re.search(pattern, compact, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip('"')
    return None


def _is_safe_identifier(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value or ""))


def get_database_debug_info() -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "backend": "postgres",
        "postgres_configured": True,
        "database_url_set": bool(DATABASE_URL),
    }
    try:
        parsed = urlparse(DATABASE_URL)
        info.update(
            {
                "host": parsed.hostname,
                "port": parsed.port,
                "database": parsed.path.lstrip("/"),
                "username": parsed.username,
            }
        )
    except ValueError as exc:
        db_logger.warning("Invalid DATABASE_URL format while building debug info.", error=str(exc))
        info.update(
            {
                "host": None,
                "port": None,
                "database": None,
                "username": None,
                "parse_error": str(exc),
            }
        )
    return info


@contextmanager
def get_conn():
    db_logger.debug("Opening database connection.")
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError as exc:
        raise RuntimeError(
            "psycopg2 is required for Supabase/Postgres connectivity but is not installed."
        ) from exc

    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    except Exception as exc:
        raise ConnectionError("Failed to connect to Supabase/Postgres using DATABASE_URL.") from exc
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


def _table_columns(conn, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table,),
        )
        return {r["column_name"] for r in cur.fetchall()}


def _ensure_column(conn, table: str, column: str, definition: str) -> None:
    if column not in _table_columns(conn, table):
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {definition}")


def _postgres_columns_need_text_migration(
    conn, columns: list[tuple[str, str]]
) -> bool:
    with conn.cursor() as cur:
        for table, column in columns:
            cur.execute(
                """
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
                """,
                (table, column),
            )
            row = cur.fetchone()
            if not row or row["data_type"] != "text":
                return True
    return False


def _sql(query: str) -> str:
    return query.replace("?", "%s")


def _executescript(conn, script: str) -> None:
    if RUNTIME_USE_POSTGRES:
        with conn.cursor() as cur:
            cur.execute(script)
        return
    conn.executescript(script)


def init_db() -> None:
    db_logger.info(
        "Initializing database schema.",
        backend="postgres",
    )
    with get_conn() as conn:
        if RUNTIME_USE_POSTGRES:
            _executescript(
                conn,
                """
                CREATE TABLE IF NOT EXISTS organizations (
                    organization_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL
                );

                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    email TEXT,
                    google_subject TEXT,
                    role TEXT NOT NULL,
                    team TEXT,
                    organization_id BIGINT,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    FOREIGN KEY(organization_id) REFERENCES organizations(organization_id)
                );

                CREATE TABLE IF NOT EXISTS modules (
                    module_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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
                    organization_id BIGINT,
                    status TEXT DEFAULT 'published',
                    learning_objectives TEXT,
                    content_sections TEXT,
                    completion_requirements TEXT,
                    quiz_required INTEGER DEFAULT 0,
                    created_by BIGINT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    FOREIGN KEY(organization_id) REFERENCES organizations(organization_id),
                    FOREIGN KEY(created_by) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS investigation_actions (
                    action_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    module_id BIGINT NOT NULL,
                    action_name TEXT NOT NULL,
                    revealed_information TEXT NOT NULL,
                    FOREIGN KEY(module_id) REFERENCES modules(module_id)
                );

                CREATE TABLE IF NOT EXISTS attempts (
                    attempt_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    module_id BIGINT NOT NULL,
                    organization_id BIGINT,
                    diagnosis_answer TEXT,
                    next_steps_answer TEXT,
                    customer_response TEXT,
                    escalation_choice TEXT,
                    notes TEXT,
                    understanding_score DOUBLE PRECISION,
                    investigation_score DOUBLE PRECISION,
                    solution_score DOUBLE PRECISION,
                    communication_score DOUBLE PRECISION,
                    total_score DOUBLE PRECISION,
                    ai_feedback TEXT,
                    strengths TEXT,
                    missed_points TEXT,
                    best_practice_reasoning TEXT,
                    recommended_response TEXT,
                    takeaway_summary TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    FOREIGN KEY(user_id) REFERENCES users(user_id),
                    FOREIGN KEY(module_id) REFERENCES modules(module_id),
                    FOREIGN KEY(organization_id) REFERENCES organizations(organization_id)
                );

                CREATE TABLE IF NOT EXISTS action_logs (
                    log_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    attempt_id BIGINT NOT NULL,
                    action_name TEXT NOT NULL,
                    "timestamp" TIMESTAMPTZ DEFAULT NOW(),
                    FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id)
                );

                CREATE TABLE IF NOT EXISTS assignments (
                    assignment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    organization_id BIGINT NOT NULL,
                    module_id BIGINT NOT NULL,
                    learner_id BIGINT NOT NULL,
                    assigned_by BIGINT,
                    due_date TEXT,
                    assigned_at TIMESTAMPTZ DEFAULT NOW(),
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
                    organization_id BIGINT,
                    last_activity TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY(organization_id) REFERENCES organizations(organization_id)
                );

                CREATE TABLE IF NOT EXISTS module_assignments (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    module_id TEXT NOT NULL,
                    organization_id BIGINT,
                    assigned_at TIMESTAMPTZ DEFAULT NOW(),
                    assigned_by TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (user_id, module_id),
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY(module_id) REFERENCES modules(id) ON DELETE CASCADE,
                    FOREIGN KEY(organization_id) REFERENCES organizations(organization_id),
                    FOREIGN KEY(assigned_by) REFERENCES users(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS module_progress (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    module_id TEXT NOT NULL,
                    organization_id BIGINT,
                    progress_percent INTEGER NOT NULL DEFAULT 0
                        CHECK (progress_percent >= 0 AND progress_percent <= 100),
                    started_at TEXT,
                    completed_at TEXT,
                    last_activity_at TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (user_id, module_id),
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY(module_id) REFERENCES modules(id) ON DELETE CASCADE,
                    FOREIGN KEY(organization_id) REFERENCES organizations(organization_id)
                );

                CREATE TABLE IF NOT EXISTS module_generation_runs (
                    run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    organization_id BIGINT NOT NULL,
                    created_by BIGINT,
                    input_title TEXT,
                    input_category TEXT,
                    input_difficulty TEXT,
                    input_description TEXT,
                    role_focus TEXT,
                    test_focus TEXT,
                    learning_objectives TEXT,
                    input_content_sections TEXT,
                    scenario_constraints TEXT,
                    completion_requirements TEXT,
                    input_quiz_required INTEGER DEFAULT 0,
                    requested_question_count INTEGER DEFAULT 5,
                    generated_title TEXT,
                    generated_description TEXT,
                    generated_scenario_overview TEXT,
                    generation_status TEXT DEFAULT 'draft',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    FOREIGN KEY(organization_id) REFERENCES organizations(organization_id),
                    FOREIGN KEY(created_by) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS module_generation_questions (
                    generated_question_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    run_id BIGINT NOT NULL,
                    question_order INTEGER NOT NULL,
                    question_text TEXT NOT NULL,
                    rationale TEXT,
                    approval_status TEXT DEFAULT 'pending',
                    admin_feedback TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    FOREIGN KEY(run_id) REFERENCES module_generation_runs(run_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS module_questions (
                    question_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    module_id BIGINT NOT NULL,
                    question_order INTEGER NOT NULL,
                    question_text TEXT NOT NULL,
                    rationale TEXT,
                    source_run_id BIGINT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    FOREIGN KEY(module_id) REFERENCES modules(module_id) ON DELETE CASCADE,
                    FOREIGN KEY(source_run_id) REFERENCES module_generation_runs(run_id) ON DELETE SET NULL
                );
                """,
            )
        else:
            _executescript(
                conn,
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
                organization_id INTEGER,
                last_activity TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(organization_id) REFERENCES organizations(organization_id)
            );

            CREATE TABLE IF NOT EXISTS module_assignments (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                module_id TEXT NOT NULL,
                organization_id INTEGER,
                assigned_at TEXT DEFAULT CURRENT_TIMESTAMP,
                assigned_by TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (user_id, module_id),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(module_id) REFERENCES modules(id) ON DELETE CASCADE,
                FOREIGN KEY(organization_id) REFERENCES organizations(organization_id),
                FOREIGN KEY(assigned_by) REFERENCES users(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS module_progress (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                module_id TEXT NOT NULL,
                organization_id INTEGER,
                progress_percent INTEGER NOT NULL DEFAULT 0
                    CHECK (progress_percent >= 0 AND progress_percent <= 100),
                started_at TEXT,
                completed_at TEXT,
                last_activity_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (user_id, module_id),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(module_id) REFERENCES modules(id) ON DELETE CASCADE,
                FOREIGN KEY(organization_id) REFERENCES organizations(organization_id)
            );

            CREATE TABLE IF NOT EXISTS module_generation_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                organization_id INTEGER NOT NULL,
                created_by INTEGER,
                input_title TEXT,
                input_category TEXT,
                input_difficulty TEXT,
                input_description TEXT,
                role_focus TEXT,
                test_focus TEXT,
                learning_objectives TEXT,
                input_content_sections TEXT,
                scenario_constraints TEXT,
                completion_requirements TEXT,
                input_quiz_required INTEGER DEFAULT 0,
                requested_question_count INTEGER DEFAULT 5,
                generated_title TEXT,
                generated_description TEXT,
                generated_scenario_overview TEXT,
                generation_status TEXT DEFAULT 'draft',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(organization_id) REFERENCES organizations(organization_id),
                FOREIGN KEY(created_by) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS module_generation_questions (
                generated_question_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                question_order INTEGER NOT NULL,
                question_text TEXT NOT NULL,
                rationale TEXT,
                approval_status TEXT DEFAULT 'pending',
                admin_feedback TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(run_id) REFERENCES module_generation_runs(run_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS module_questions (
                question_id INTEGER PRIMARY KEY AUTOINCREMENT,
                module_id INTEGER NOT NULL,
                question_order INTEGER NOT NULL,
                question_text TEXT NOT NULL,
                rationale TEXT,
                source_run_id INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(module_id) REFERENCES modules(module_id) ON DELETE CASCADE,
                FOREIGN KEY(source_run_id) REFERENCES module_generation_runs(run_id) ON DELETE SET NULL
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
        if RUNTIME_USE_POSTGRES:
            text_migration_columns = [
                ("learner_profiles", "user_id"),
                ("module_assignments", "user_id"),
                ("module_progress", "user_id"),
                ("users", "id"),
            ]
            with conn.cursor() as cur:
                if _postgres_columns_need_text_migration(conn, text_migration_columns):
                    cur.execute("DROP VIEW IF EXISTS learner_dashboard_summary")
                    cur.execute(
                        """
                        ALTER TABLE learner_profiles
                        ALTER COLUMN user_id TYPE TEXT USING user_id::TEXT
                        """
                    )
                    cur.execute(
                        """
                        ALTER TABLE module_assignments
                        ALTER COLUMN user_id TYPE TEXT USING user_id::TEXT
                        """
                    )
                    cur.execute(
                        """
                        ALTER TABLE module_progress
                        ALTER COLUMN user_id TYPE TEXT USING user_id::TEXT
                        """
                    )
                    cur.execute(
                        """
                        ALTER TABLE users
                        ALTER COLUMN id TYPE TEXT USING id::TEXT
                        """
                    )
        if RUNTIME_USE_POSTGRES:
            with conn.cursor() as cur:
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_external_id ON users(id)")
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users(username)")
        else:
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
        _ensure_column(conn, "learner_profiles", "organization_id", "INTEGER")
        _ensure_column(conn, "module_assignments", "organization_id", "INTEGER")
        _ensure_column(conn, "module_progress", "organization_id", "INTEGER")
        if RUNTIME_USE_POSTGRES:
            with conn.cursor() as cur:
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_modules_external_id ON modules(id)")
                cur.execute(
                    """
                    UPDATE learner_profiles lp
                    SET organization_id = u.organization_id
                    FROM users u
                    WHERE lp.user_id = u.id
                      AND lp.organization_id IS NULL
                    """
                )
                cur.execute(
                    """
                    UPDATE module_assignments ma
                    SET organization_id = u.organization_id
                    FROM users u
                    WHERE ma.user_id = u.id
                      AND ma.organization_id IS NULL
                    """
                )
                cur.execute(
                    """
                    UPDATE module_progress mp
                    SET organization_id = u.organization_id
                    FROM users u
                    WHERE mp.user_id = u.id
                      AND mp.organization_id IS NULL
                    """
                )
        else:
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_modules_external_id ON modules(id)")
            conn.execute(
                """
                UPDATE learner_profiles
                SET organization_id = (
                    SELECT u.organization_id
                    FROM users u
                    WHERE u.id = learner_profiles.user_id
                )
                WHERE organization_id IS NULL
                """
            )
            conn.execute(
                """
                UPDATE module_assignments
                SET organization_id = (
                    SELECT u.organization_id
                    FROM users u
                    WHERE u.id = module_assignments.user_id
                )
                WHERE organization_id IS NULL
                """
            )
            conn.execute(
                """
                UPDATE module_progress
                SET organization_id = (
                    SELECT u.organization_id
                    FROM users u
                    WHERE u.id = module_progress.user_id
                )
                WHERE organization_id IS NULL
                """
            )

        _ensure_column(conn, "attempts", "organization_id", "INTEGER")
        _ensure_column(conn, "module_generation_runs", "generation_status", "TEXT DEFAULT 'draft'")
        _ensure_column(conn, "module_generation_runs", "input_content_sections", "TEXT")
        _ensure_column(conn, "module_generation_runs", "input_quiz_required", "INTEGER DEFAULT 0")
        _ensure_column(conn, "module_generation_questions", "approval_status", "TEXT DEFAULT 'pending'")

        if RUNTIME_USE_POSTGRES:
            with conn.cursor() as cur:
                cur.execute("CREATE INDEX IF NOT EXISTS idx_learner_profiles_user_id ON learner_profiles(user_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_learner_profiles_status ON learner_profiles(status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_module_assignments_user_id ON module_assignments(user_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_module_assignments_module_id ON module_assignments(module_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_module_progress_user_id ON module_progress(user_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_module_progress_module_id ON module_progress(module_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_module_progress_completed_at ON module_progress(completed_at)")
                cur.execute(
                    """
                    CREATE OR REPLACE VIEW learner_dashboard_summary AS
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
                        lp.last_activity
                    """
                )
        else:
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


def fetch_all(query: str, params: Iterable[Any] = ()) -> List[Dict[str, Any]]:
    try:
        with get_conn() as conn:
            if RUNTIME_USE_POSTGRES:
                with conn.cursor() as cur:
                    cur.execute(_sql(query), tuple(params))
                    rows = cur.fetchall()
            else:
                cur = conn.execute(query, params)
                rows = cur.fetchall()
            db_logger.debug("Database read completed.", operation="fetch_all", row_count=len(rows))
            return rows
    except Exception:
        db_logger.exception("Database read failed.", operation="fetch_all")
        raise


def fetch_one(query: str, params: Iterable[Any] = ()) -> Optional[Dict[str, Any]]:
    try:
        with get_conn() as conn:
            if RUNTIME_USE_POSTGRES:
                with conn.cursor() as cur:
                    cur.execute(_sql(query), tuple(params))
                    row = cur.fetchone()
            else:
                cur = conn.execute(query, params)
                row = cur.fetchone()
            db_logger.debug("Database read completed.", operation="fetch_one", found=bool(row))
            return row
    except Exception:
        db_logger.exception("Database read failed.", operation="fetch_one")
        raise


def execute(query: str, params: Iterable[Any] = ()) -> int:
    try:
        params_tuple = tuple(params)
        target_table = _infer_target_table(query)
        query_preview = " ".join(query.split())[:200]
        db_logger.info(
            "Database write starting.",
            operation="execute",
            backend="postgres",
            target_table=target_table,
            param_count=len(params_tuple),
            query_preview=query_preview,
        )
        with get_conn() as conn:
            if RUNTIME_USE_POSTGRES:
                with conn.cursor() as cur:
                    cur.execute(_sql(query), params_tuple)
                    if query.lstrip().upper().startswith("INSERT"):
                        cur.execute("SELECT LASTVAL() AS id")
                        lastrow = cur.fetchone()
                        lastrowid = int(lastrow["id"]) if lastrow else 0
                    else:
                        lastrowid = 0
            else:
                cur = conn.execute(query, params_tuple)
                lastrowid = cur.lastrowid
            db_logger.info(
                "Database write completed.",
                operation="execute",
                backend="postgres",
                target_table=target_table,
                lastrowid=lastrowid,
            )
            return lastrowid
    except Exception:
        db_logger.exception("Database write failed.", operation="execute")
        raise


def executemany(query: str, rows: Iterable[Iterable[Any]]) -> None:
    try:
        buffered_rows = list(rows)
        with get_conn() as conn:
            if RUNTIME_USE_POSTGRES:
                with conn.cursor() as cur:
                    cur.executemany(_sql(query), [tuple(r) for r in buffered_rows])
            else:
                conn.executemany(query, buffered_rows)
            db_logger.info("Database bulk write completed.", operation="executemany", row_count=len(buffered_rows))
    except Exception:
        db_logger.exception("Database bulk write failed.", operation="executemany")
        raise


def list_public_tables() -> list[str]:
    with get_conn() as conn:
        if RUNTIME_USE_POSTGRES:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                    """
                )
                rows = cur.fetchall()
                return [str(r["table_name"]) for r in rows]

        rows = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        ).fetchall()
        return [str(r["name"]) for r in rows]


def fetch_table_rows(table_name: str, row_limit: int | None = None) -> list[Dict[str, Any]]:
    if not _is_safe_identifier(table_name):
        raise ValueError(f"Unsafe table name: {table_name}")

    query = f"SELECT * FROM {table_name}"
    params: tuple[Any, ...] = ()
    if row_limit is not None and row_limit > 0:
        query += " LIMIT ?"
        params = (row_limit,)
    return fetch_all(query, params)


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
