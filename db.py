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
        "can_connect": False,
        "connect_reason": "Connection has not been attempted yet.",
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
                "connect_reason": f"Invalid DATABASE_URL format: {exc}",
            }
        )
        return info

    if not DATABASE_URL:
        info["connect_reason"] = "DATABASE_URL is not configured."
        return info

    try:
        import psycopg2
    except ImportError:
        info["connect_reason"] = "psycopg2 is not installed."
        return info

    try:
        with psycopg2.connect(DATABASE_URL, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok;")
                row = cur.fetchone()
        info["can_connect"] = bool(row and row[0] == 1)
        info["connect_reason"] = (
            "Connection established and validation query succeeded."
            if info["can_connect"]
            else "Connection opened but validation query returned an unexpected result."
        )
    except Exception as exc:
        info["connect_reason"] = f"{type(exc).__name__}: {exc}"

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


def _ensure_column(conn, table: str, column: str, definition: str) -> bool:
    if not _is_safe_identifier(table):
        raise ValueError(f"Unsafe table identifier: {table}")
    if not _is_safe_identifier(column):
        raise ValueError(f"Unsafe column identifier: {column}")
    if column not in _table_columns(conn, table):
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {definition}")
        return True
    return False


def _table_exists_conn(conn, table_name: str) -> bool:
    if not _is_safe_identifier(table_name):
        raise ValueError(f"Unsafe table name: {table_name}")
    if RUNTIME_USE_POSTGRES:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s) AS regclass_name", (f"public.{table_name}",))
            row = cur.fetchone()
            return bool(row and row.get("regclass_name"))
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        (table_name,),
    ).fetchone()
    return bool(row)


def table_exists(table_name: str) -> bool:
    with get_conn() as conn:
        return _table_exists_conn(conn, table_name)


def ensure_module_rubric_criteria_table(conn: Any = None) -> None:
    if conn is None:
        with get_conn() as managed_conn:
            ensure_module_rubric_criteria_table(managed_conn)
        return
    if RUNTIME_USE_POSTGRES:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS module_rubric_criteria (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    module_id BIGINT NOT NULL,
                    question_id BIGINT NOT NULL,
                    criterion_order INTEGER NOT NULL DEFAULT 1,
                    label TEXT NOT NULL,
                    description TEXT DEFAULT '',
                    max_score NUMERIC DEFAULT 1.0,
                    weight NUMERIC DEFAULT 1.0,
                    feedback TEXT DEFAULT '',
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_module_rubric_criteria_module_id
                ON module_rubric_criteria(module_id)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_module_rubric_criteria_question_id
                ON module_rubric_criteria(question_id)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_module_rubric_criteria_module_question
                ON module_rubric_criteria(module_id, question_id)
                """
            )
        _ensure_column(conn, "module_rubric_criteria", "max_points", "NUMERIC DEFAULT 1.0")
        _ensure_column(conn, "module_rubric_criteria", "grading_guidance", "TEXT DEFAULT ''")
    else:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS module_rubric_criteria (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                module_id INTEGER NOT NULL,
                question_id INTEGER NOT NULL,
                criterion_order INTEGER NOT NULL DEFAULT 1,
                label TEXT NOT NULL,
                description TEXT DEFAULT '',
                max_score REAL DEFAULT 1.0,
                weight REAL DEFAULT 1.0,
                feedback TEXT DEFAULT '',
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_module_rubric_criteria_module_id ON module_rubric_criteria(module_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_module_rubric_criteria_question_id ON module_rubric_criteria(question_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_module_rubric_criteria_module_question ON module_rubric_criteria(module_id, question_id)"
        )
        _ensure_column(conn, "module_rubric_criteria", "max_points", "REAL DEFAULT 1.0")
        _ensure_column(conn, "module_rubric_criteria", "grading_guidance", "TEXT DEFAULT ''")


def ensure_question_conversation_messages_table(conn: Any = None) -> None:
    if conn is None:
        with get_conn() as managed_conn:
            ensure_question_conversation_messages_table(managed_conn)
        return
    if RUNTIME_USE_POSTGRES:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS question_conversation_messages (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    attempt_id BIGINT NOT NULL,
                    question_id BIGINT NOT NULL,
                    message_role TEXT NOT NULL,
                    message_content TEXT NOT NULL,
                    message_order INTEGER NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                    FOREIGN KEY(question_id) REFERENCES module_questions(question_id) ON DELETE CASCADE
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_question_conversation_messages_attempt_id
                ON question_conversation_messages(attempt_id)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_question_conversation_messages_question_id
                ON question_conversation_messages(question_id)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_question_conversation_messages_attempt_question
                ON question_conversation_messages(attempt_id, question_id)
                """
            )
    else:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS question_conversation_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                attempt_id INTEGER NOT NULL,
                question_id INTEGER NOT NULL,
                message_role TEXT NOT NULL,
                message_content TEXT NOT NULL,
                message_order INTEGER NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                FOREIGN KEY(question_id) REFERENCES module_questions(question_id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_question_conversation_messages_attempt_id ON question_conversation_messages(attempt_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_question_conversation_messages_question_id ON question_conversation_messages(question_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_question_conversation_messages_attempt_question ON question_conversation_messages(attempt_id, question_id)"
        )


def _ensure_attempts_result_approver_fk_postgres(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'attempts_result_approved_by_user_fk'
              ) THEN
                ALTER TABLE attempts
                ADD CONSTRAINT attempts_result_approved_by_user_fk
                FOREIGN KEY (result_approved_by_user_id) REFERENCES users(user_id);
              END IF;
            END
            $$;
            """
        )


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


def _postgres_column_data_type(conn, table: str, column: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
            """,
            (table, column),
        )
        row = cur.fetchone()
        return row["data_type"] if row else None


def _migrate_input_quiz_required_to_boolean(conn) -> None:
    current_type = _postgres_column_data_type(conn, "module_generation_runs", "input_quiz_required")
    if current_type in {None, "boolean"}:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            ALTER TABLE module_generation_runs
            ALTER COLUMN input_quiz_required TYPE BOOLEAN
            USING CASE
              WHEN input_quiz_required IS NULL THEN NULL
              WHEN input_quiz_required = 0 THEN FALSE
              ELSE TRUE
            END
            """
        )
        cur.execute(
            """
            ALTER TABLE module_generation_runs
            ALTER COLUMN input_quiz_required SET DEFAULT FALSE
            """
        )


def _migrate_submitted_state_to_integer(conn) -> None:
    current_type = _postgres_column_data_type(conn, "assignment_workspace_state", "submitted_state")
    if current_type in {None, "integer"}:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            ALTER TABLE assignment_workspace_state
            ALTER COLUMN submitted_state DROP DEFAULT
            """
        )
        cur.execute(
            """
            ALTER TABLE assignment_workspace_state
            ALTER COLUMN submitted_state TYPE INTEGER
            USING CASE
              WHEN submitted_state IS NULL THEN 0
              WHEN LOWER(BTRIM(submitted_state::text)) IN ('approved', '2') THEN 2
              WHEN LOWER(BTRIM(submitted_state::text)) IN ('true', 't', '1', 'yes', 'y', 'submitted', 'complete') THEN 1
              ELSE 0
            END
            """
        )
        cur.execute(
            """
            ALTER TABLE assignment_workspace_state
            ALTER COLUMN submitted_state SET DEFAULT 0
            """
        )


def _sql(query: str) -> str:
    return query.replace("?", "%s")


def _executescript(conn, script: str) -> None:
    if RUNTIME_USE_POSTGRES:
        with conn.cursor() as cur:
            cur.execute(script)
        return
    conn.executescript(script)


def init_db() -> None:
    db_logger.info("Running init_db (should only happen once)")
    db_logger.info(
        "Initializing database schema.",
        backend="postgres",
    )
    with get_conn() as conn:
        lock_acquired = False
        try:
            if RUNTIME_USE_POSTGRES:
                with conn.cursor() as cur:
                    cur.execute("SET lock_timeout = '2s';")
                    cur.execute(
                        """
                        SELECT pg_advisory_lock(123456);
                        """
                    )
                lock_acquired = True
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
                    email_notifications_enabled INTEGER DEFAULT 0,
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
                    status TEXT DEFAULT 'existing',
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
                    started_at TIMESTAMPTZ,
                    submitted_at TIMESTAMPTZ,
                    elapsed_seconds INTEGER,
                    time_limit_seconds INTEGER,
                    time_remaining_seconds INTEGER,
                    attempt_state TEXT DEFAULT 'submitted',
                    graded_by_type TEXT,
                    graded_by_user_id BIGINT,
                    graded_at TIMESTAMPTZ,
                    result_status TEXT NOT NULL DEFAULT 'submitted'
                        CHECK (
                            result_status IN (
                                'submitted',
                                'ai_grading',
                                'ai_graded_pending_review',
                                'pending_review',
                                'approved',
                                'returned',
                                'grading_failed'
                            )
                        ),
                    result_approved_at TIMESTAMPTZ,
                    result_approved_by_user_id BIGINT,
                    diagnosis_answer TEXT,
                    next_steps_answer TEXT,
                    customer_response TEXT,
                    escalation_choice TEXT,
                    notes TEXT,
                    timed_out INTEGER DEFAULT 0,
                    question_responses TEXT,
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
                    FOREIGN KEY(organization_id) REFERENCES organizations(organization_id),
                    FOREIGN KEY(graded_by_user_id) REFERENCES users(user_id),
                    FOREIGN KEY(result_approved_by_user_id) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS action_logs (
                    log_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    attempt_id BIGINT NOT NULL,
                    action_name TEXT NOT NULL,
                    "timestamp" TIMESTAMPTZ DEFAULT NOW(),
                    FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id)
                );

                CREATE TABLE IF NOT EXISTS submission_scores (
                    submission_score_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    attempt_id BIGINT NOT NULL UNIQUE,
                    scoring_version TEXT NOT NULL DEFAULT 'heuristic_v1',
                    understanding_score DOUBLE PRECISION,
                    investigation_score DOUBLE PRECISION,
                    solution_score DOUBLE PRECISION,
                    communication_score DOUBLE PRECISION,
                    understanding_rationale TEXT,
                    investigation_rationale TEXT,
                    solution_rationale TEXT,
                    communication_rationale TEXT,
                    total_score DOUBLE PRECISION,
                    scoring_provider TEXT,
                    scoring_model_name TEXT,
                    scoring_prompt_template_id TEXT,
                    scoring_temperature DOUBLE PRECISION,
                    scoring_config_json TEXT,
                    scored_at TIMESTAMPTZ DEFAULT NOW(),
                    score_inputs_json TEXT,
                    ai_total_score DOUBLE PRECISION,
                    admin_total_score DOUBLE PRECISION,
                    final_total_score DOUBLE PRECISION,
                    score_status TEXT DEFAULT 'pending',
                    max_total_score DOUBLE PRECISION,
                    percentage DOUBLE PRECISION,
                    grading_status TEXT DEFAULT 'submitted',
                    review_status TEXT DEFAULT 'submitted',
                    overall_ai_feedback TEXT,
                    overall_admin_feedback TEXT,
                    learner_visible_feedback TEXT,
                    best_practice_reasoning TEXT,
                    recommended_response TEXT,
                    lesson_takeaway TEXT,
                    learner_strengths TEXT,
                    learner_weaknesses TEXT,
                    learner_missed_points TEXT,
                    show_results_to_learner BOOLEAN DEFAULT FALSE,
                    show_overall_score_to_learner BOOLEAN DEFAULT FALSE,
                    show_question_scores_to_learner BOOLEAN DEFAULT FALSE,
                    show_feedback_to_learner BOOLEAN DEFAULT FALSE,
                    show_expected_answers_to_learner BOOLEAN DEFAULT FALSE,
                    show_grading_criteria_to_learner BOOLEAN DEFAULT FALSE,
                    show_ai_evaluation_details_to_learner BOOLEAN DEFAULT FALSE,
                    show_ai_review_to_learner BOOLEAN DEFAULT FALSE,
                    show_learner_responses_to_learner BOOLEAN DEFAULT FALSE,
                    results_visibility_json JSONB DEFAULT '{}'::jsonb,
                    approved_by BIGINT,
                    approved_at TIMESTAMPTZ,
                    FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS submission_question_scores (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    attempt_id BIGINT NOT NULL,
                    question_id BIGINT,
                    ai_score DOUBLE PRECISION,
                    admin_score DOUBLE PRECISION,
                    final_score DOUBLE PRECISION,
                    feedback TEXT,
                    learner_answer TEXT,
                    ai_awarded_points DOUBLE PRECISION,
                    ai_max_points DOUBLE PRECISION,
                    ai_feedback TEXT,
                    ai_reasoning TEXT,
                    missing_key_concepts TEXT,
                    admin_awarded_points DOUBLE PRECISION,
                    admin_feedback TEXT,
                    final_awarded_points DOUBLE PRECISION,
                    visible_to_learner BOOLEAN DEFAULT FALSE,
                    is_admin_override BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(attempt_id, question_id),
                    FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                    FOREIGN KEY(question_id) REFERENCES module_questions(question_id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS submission_regrade_history (
                    regrade_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    attempt_id BIGINT NOT NULL,
                    old_total_score DOUBLE PRECISION,
                    new_total_score DOUBLE PRECISION,
                    old_category_scores_json TEXT,
                    new_category_scores_json TEXT,
                    reason TEXT,
                    changed_by_type TEXT NOT NULL DEFAULT 'admin',
                    changed_by_user_id BIGINT,
                    changed_at TIMESTAMPTZ DEFAULT NOW(),
                    FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                    FOREIGN KEY(changed_by_user_id) REFERENCES users(user_id)
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

                CREATE TABLE IF NOT EXISTS assignment_workspace_state (
                    assignment_id BIGINT PRIMARY KEY,
                    organization_id BIGINT NOT NULL,
                    module_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    current_step INTEGER DEFAULT 1,
                    progress_status TEXT DEFAULT 'not_started',
                    learner_notes TEXT,
                    diagnosis_response TEXT,
                    next_steps_response TEXT,
                    customer_response TEXT,
                    escalation_choice TEXT,
                    question_responses TEXT DEFAULT '{}',
                    revealed_actions TEXT DEFAULT '{}',
                    used_actions TEXT DEFAULT '[]',
                    submitted_state INTEGER DEFAULT 0,
                    started_at TEXT,
                    submitted_at TIMESTAMPTZ,
                    last_saved_at TIMESTAMPTZ DEFAULT NOW(),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    FOREIGN KEY(assignment_id) REFERENCES assignments(assignment_id) ON DELETE CASCADE,
                    FOREIGN KEY(organization_id) REFERENCES organizations(organization_id),
                    FOREIGN KEY(module_id) REFERENCES modules(module_id),
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
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
                    input_quiz_required BOOLEAN DEFAULT FALSE,
                    requested_question_count INTEGER DEFAULT 5,
                    input_estimated_minutes INTEGER,
                    generated_title TEXT,
                    generated_description TEXT,
                    generated_scenario_overview TEXT,
                    generation_status TEXT DEFAULT 'pending',
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
                    question_type TEXT DEFAULT 'open_text',
                    options_text TEXT,
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
                    expected_answer TEXT,
                    rubric TEXT,
                    partial_credit_guidance TEXT,
                    incorrect_criteria TEXT,
                    incomplete_criteria TEXT,
                    strong_response_criteria TEXT,
                    max_points DOUBLE PRECISION DEFAULT 10,
                    question_type TEXT DEFAULT 'open_text',
                    options_text TEXT,
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
                email_notifications_enabled INTEGER DEFAULT 0,
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
                status TEXT DEFAULT 'existing',
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
                started_at TEXT,
                submitted_at TEXT,
                elapsed_seconds INTEGER,
                time_limit_seconds INTEGER,
                time_remaining_seconds INTEGER,
                attempt_state TEXT DEFAULT 'submitted',
                graded_by_type TEXT,
                graded_by_user_id INTEGER,
                graded_at TEXT,
                result_status TEXT NOT NULL DEFAULT 'submitted'
                    CHECK (result_status IN ('submitted', 'ai_grading', 'ai_graded_pending_review', 'pending_review', 'approved', 'returned', 'grading_failed')),
                result_approved_at TEXT,
                result_approved_by_user_id INTEGER,
                diagnosis_answer TEXT,
                next_steps_answer TEXT,
                customer_response TEXT,
                escalation_choice TEXT,
                notes TEXT,
                timed_out INTEGER DEFAULT 0,
                question_responses TEXT,
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
                FOREIGN KEY(organization_id) REFERENCES organizations(organization_id),
                FOREIGN KEY(graded_by_user_id) REFERENCES users(user_id),
                FOREIGN KEY(result_approved_by_user_id) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS action_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                attempt_id INTEGER NOT NULL,
                action_name TEXT NOT NULL,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id)
            );

            CREATE TABLE IF NOT EXISTS submission_scores (
                submission_score_id INTEGER PRIMARY KEY AUTOINCREMENT,
                attempt_id INTEGER NOT NULL UNIQUE,
                scoring_version TEXT NOT NULL DEFAULT 'heuristic_v1',
                understanding_score REAL,
                investigation_score REAL,
                solution_score REAL,
                communication_score REAL,
                understanding_rationale TEXT,
                investigation_rationale TEXT,
                solution_rationale TEXT,
                communication_rationale TEXT,
                total_score REAL,
                scoring_provider TEXT,
                scoring_model_name TEXT,
                scoring_prompt_template_id TEXT,
                scoring_temperature REAL,
                scoring_config_json TEXT,
                scored_at TEXT DEFAULT CURRENT_TIMESTAMP,
                score_inputs_json TEXT,
                ai_total_score REAL,
                admin_total_score REAL,
                final_total_score REAL,
                score_status TEXT DEFAULT 'pending',
                max_total_score REAL,
                percentage REAL,
                grading_status TEXT DEFAULT 'submitted',
                review_status TEXT DEFAULT 'submitted',
                overall_ai_feedback TEXT,
                overall_admin_feedback TEXT,
                learner_visible_feedback TEXT,
                best_practice_reasoning TEXT,
                recommended_response TEXT,
                lesson_takeaway TEXT,
                learner_strengths TEXT,
                learner_weaknesses TEXT,
                learner_missed_points TEXT,
                show_results_to_learner INTEGER DEFAULT 0,
                show_overall_score_to_learner INTEGER DEFAULT 0,
                show_question_scores_to_learner INTEGER DEFAULT 0,
                show_feedback_to_learner INTEGER DEFAULT 0,
                show_expected_answers_to_learner INTEGER DEFAULT 0,
                show_grading_criteria_to_learner INTEGER DEFAULT 0,
                show_ai_evaluation_details_to_learner INTEGER DEFAULT 0,
                show_ai_review_to_learner INTEGER DEFAULT 0,
                show_learner_responses_to_learner INTEGER DEFAULT 0,
                results_visibility_json TEXT,
                approved_by INTEGER,
                approved_at TEXT,
                FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS submission_question_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                attempt_id INTEGER NOT NULL,
                question_id INTEGER,
                ai_score REAL,
                admin_score REAL,
                final_score REAL,
                feedback TEXT,
                learner_answer TEXT,
                ai_awarded_points REAL,
                ai_max_points REAL,
                ai_feedback TEXT,
                ai_reasoning TEXT,
                missing_key_concepts TEXT,
                admin_awarded_points REAL,
                admin_feedback TEXT,
                final_awarded_points REAL,
                visible_to_learner INTEGER DEFAULT 0,
                is_admin_override INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(attempt_id, question_id),
                FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                FOREIGN KEY(question_id) REFERENCES module_questions(question_id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS submission_regrade_history (
                regrade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                attempt_id INTEGER NOT NULL,
                old_total_score REAL,
                new_total_score REAL,
                old_category_scores_json TEXT,
                new_category_scores_json TEXT,
                reason TEXT,
                changed_by_type TEXT NOT NULL DEFAULT 'admin',
                changed_by_user_id INTEGER,
                changed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                FOREIGN KEY(changed_by_user_id) REFERENCES users(user_id)
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

            CREATE TABLE IF NOT EXISTS assignment_workspace_state (
                assignment_id INTEGER PRIMARY KEY,
                organization_id INTEGER NOT NULL,
                module_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                current_step INTEGER DEFAULT 1,
                progress_status TEXT DEFAULT 'not_started',
                learner_notes TEXT,
                diagnosis_response TEXT,
                next_steps_response TEXT,
                customer_response TEXT,
                escalation_choice TEXT,
                question_responses TEXT DEFAULT '{}',
                revealed_actions TEXT DEFAULT '{}',
                used_actions TEXT DEFAULT '[]',
                submitted_state INTEGER DEFAULT 0,
                started_at TEXT,
                submitted_at TEXT,
                last_saved_at TEXT DEFAULT CURRENT_TIMESTAMP,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(assignment_id) REFERENCES assignments(assignment_id) ON DELETE CASCADE,
                FOREIGN KEY(organization_id) REFERENCES organizations(organization_id),
                FOREIGN KEY(module_id) REFERENCES modules(module_id),
                FOREIGN KEY(user_id) REFERENCES users(user_id)
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
                input_estimated_minutes INTEGER,
                generated_title TEXT,
                generated_description TEXT,
                generated_scenario_overview TEXT,
                generation_status TEXT DEFAULT 'pending',
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
                question_type TEXT DEFAULT 'open_text',
                options_text TEXT,
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
                expected_answer TEXT,
                rubric TEXT,
                partial_credit_guidance TEXT,
                incorrect_criteria TEXT,
                incomplete_criteria TEXT,
                strong_response_criteria TEXT,
                max_points REAL DEFAULT 10,
                question_type TEXT DEFAULT 'open_text',
                options_text TEXT,
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
            _ensure_column(conn, "users", "email_notifications_enabled", "INTEGER DEFAULT 0")
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
            _ensure_column(conn, "modules", "status", "TEXT DEFAULT 'existing'")
            _ensure_column(conn, "modules", "learning_objectives", "TEXT")
            _ensure_column(conn, "modules", "content_sections", "TEXT")
            _ensure_column(conn, "modules", "completion_requirements", "TEXT")
            _ensure_column(conn, "modules", "quiz_required", "INTEGER DEFAULT 0")
            _ensure_column(conn, "modules", "created_by", "INTEGER")
            _ensure_column(conn, "modules", "created_at", "TEXT DEFAULT CURRENT_TIMESTAMP")
            _ensure_column(conn, "modules", "updated_at", "TEXT DEFAULT CURRENT_TIMESTAMP")
            _ensure_column(conn, "modules", "id", "TEXT")
            if RUNTIME_USE_POSTGRES:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE modules
                        SET status = CASE
                            WHEN LOWER(BTRIM(COALESCE(status, ''))) = 'archived' THEN 'archived'
                            ELSE 'existing'
                        END
                        """
                    )
                    cur.execute("ALTER TABLE modules ALTER COLUMN status SET DEFAULT 'existing'")
                    cur.execute(
                        """
                        DO $$
                        BEGIN
                          IF NOT EXISTS (
                            SELECT 1
                            FROM pg_constraint
                            WHERE conname = 'modules_status_valid'
                          ) THEN
                            ALTER TABLE modules
                            ADD CONSTRAINT modules_status_valid
                            CHECK (status IN ('existing', 'archived'));
                          END IF;
                        END
                        $$;
                        """
                    )
            else:
                conn.execute(
                    """
                    UPDATE modules
                    SET status = CASE
                        WHEN LOWER(TRIM(COALESCE(status, ''))) = 'archived' THEN 'archived'
                        ELSE 'existing'
                    END
                    """
                )
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
            _ensure_column(conn, "attempts", "started_at", "TIMESTAMPTZ")
            _ensure_column(conn, "attempts", "submitted_at", "TIMESTAMPTZ")
            _ensure_column(conn, "attempts", "elapsed_seconds", "INTEGER")
            _ensure_column(conn, "attempts", "time_limit_seconds", "INTEGER")
            _ensure_column(conn, "attempts", "time_remaining_seconds", "INTEGER")
            _ensure_column(conn, "attempts", "attempt_state", "TEXT DEFAULT 'submitted'")
            _ensure_column(conn, "attempts", "graded_by_type", "TEXT")
            _ensure_column(conn, "attempts", "graded_by_user_id", "BIGINT")
            _ensure_column(conn, "attempts", "graded_at", "TIMESTAMPTZ")
            result_status_added = _ensure_column(
                conn,
                "attempts",
                "result_status",
                "TEXT DEFAULT 'submitted'",
            )
            result_approved_at_added = _ensure_column(conn, "attempts", "result_approved_at", "TIMESTAMPTZ")
            result_approved_by_added = _ensure_column(conn, "attempts", "result_approved_by_user_id", "BIGINT")
            _ensure_column(conn, "attempts", "timed_out", "INTEGER DEFAULT 0")
            _ensure_column(conn, "attempts", "question_responses", "TEXT")
            if RUNTIME_USE_POSTGRES:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE attempts
                        SET result_status = CASE
                            WHEN LOWER(BTRIM(COALESCE(result_status, ''))) IN (
                                'submitted', 'ai_grading', 'ai_graded_pending_review', 'pending_review', 'approved', 'returned', 'grading_failed'
                            )
                                THEN LOWER(BTRIM(result_status))
                            WHEN total_score IS NOT NULL
                                THEN 'ai_graded_pending_review'
                            ELSE 'pending_review'
                        END
                        """
                    )
                    cur.execute("ALTER TABLE attempts ALTER COLUMN result_status SET DEFAULT 'submitted'")
                    cur.execute("ALTER TABLE attempts DROP CONSTRAINT IF EXISTS attempts_result_status_valid")
                    cur.execute(
                        """
                        ALTER TABLE attempts
                        ADD CONSTRAINT attempts_result_status_valid
                        CHECK (
                            result_status IN (
                                'submitted',
                                'ai_grading',
                                'ai_graded_pending_review',
                                'pending_review',
                                'approved',
                                'returned',
                                'grading_failed'
                            )
                        );
                        """
                    )
                _ensure_attempts_result_approver_fk_postgres(conn)
            else:
                conn.execute(
                    """
                    UPDATE attempts
                    SET result_status = CASE
                        WHEN LOWER(TRIM(COALESCE(result_status, ''))) IN ('submitted', 'ai_grading', 'ai_graded_pending_review', 'pending_review', 'approved', 'returned', 'grading_failed')
                            THEN LOWER(TRIM(result_status))
                        WHEN total_score IS NOT NULL
                            THEN 'ai_graded_pending_review'
                        ELSE 'pending_review'
                    END
                    """
                )
            should_backfill_legacy_approvals = (
                result_status_added or result_approved_at_added or result_approved_by_added
            )
            if should_backfill_legacy_approvals:
                if RUNTIME_USE_POSTGRES:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE attempts
                            SET result_status = 'approved',
                                result_approved_at = CURRENT_TIMESTAMP,
                                result_approved_by_user_id = NULL
                            WHERE total_score IS NOT NULL
                              AND COALESCE(result_status, 'pending_review') = 'pending_review'
                              AND result_approved_at IS NULL
                              AND result_approved_by_user_id IS NULL
                            """
                        )
                else:
                    conn.execute(
                        """
                        UPDATE attempts
                        SET result_status = 'approved',
                            result_approved_at = CURRENT_TIMESTAMP,
                            result_approved_by_user_id = NULL
                        WHERE total_score IS NOT NULL
                          AND COALESCE(result_status, 'pending_review') = 'pending_review'
                          AND result_approved_at IS NULL
                          AND result_approved_by_user_id IS NULL
                        """
                    )
            _ensure_column(conn, "submission_scores", "score_inputs_json", "TEXT")
            _ensure_column(conn, "submission_scores", "understanding_rationale", "TEXT")
            _ensure_column(conn, "submission_scores", "investigation_rationale", "TEXT")
            _ensure_column(conn, "submission_scores", "solution_rationale", "TEXT")
            _ensure_column(conn, "submission_scores", "communication_rationale", "TEXT")
            _ensure_column(conn, "submission_scores", "scoring_provider", "TEXT")
            _ensure_column(conn, "submission_scores", "scoring_model_name", "TEXT")
            _ensure_column(conn, "submission_scores", "scoring_prompt_template_id", "TEXT")
            _ensure_column(conn, "submission_scores", "scoring_temperature", "DOUBLE PRECISION")
            _ensure_column(conn, "submission_scores", "scoring_config_json", "TEXT")
            _ensure_column(conn, "submission_scores", "ai_total_score", "DOUBLE PRECISION")
            _ensure_column(conn, "submission_scores", "admin_total_score", "DOUBLE PRECISION")
            _ensure_column(conn, "submission_scores", "final_total_score", "DOUBLE PRECISION")
            _ensure_column(conn, "submission_scores", "score_status", "TEXT DEFAULT 'pending'")
            _ensure_column(conn, "submission_scores", "max_total_score", "DOUBLE PRECISION")
            _ensure_column(conn, "submission_scores", "percentage", "DOUBLE PRECISION")
            _ensure_column(conn, "submission_scores", "grading_status", "TEXT DEFAULT 'submitted'")
            _ensure_column(conn, "submission_scores", "review_status", "TEXT DEFAULT 'submitted'")
            _ensure_column(conn, "submission_scores", "overall_ai_feedback", "TEXT")
            _ensure_column(conn, "submission_scores", "overall_admin_feedback", "TEXT")
            _ensure_column(conn, "submission_scores", "learner_visible_feedback", "TEXT")
            _ensure_column(conn, "submission_scores", "best_practice_reasoning", "TEXT")
            _ensure_column(conn, "submission_scores", "recommended_response", "TEXT")
            _ensure_column(conn, "submission_scores", "lesson_takeaway", "TEXT")
            _ensure_column(conn, "submission_scores", "learner_strengths", "TEXT")
            _ensure_column(conn, "submission_scores", "learner_weaknesses", "TEXT")
            _ensure_column(conn, "submission_scores", "learner_missed_points", "TEXT")
            _ensure_column(
                conn,
                "submission_scores",
                "show_results_to_learner",
                "BOOLEAN DEFAULT FALSE" if RUNTIME_USE_POSTGRES else "INTEGER DEFAULT 0",
            )
            _ensure_column(
                conn,
                "submission_scores",
                "show_overall_score_to_learner",
                "BOOLEAN DEFAULT FALSE" if RUNTIME_USE_POSTGRES else "INTEGER DEFAULT 0",
            )
            _ensure_column(
                conn,
                "submission_scores",
                "show_question_scores_to_learner",
                "BOOLEAN DEFAULT FALSE" if RUNTIME_USE_POSTGRES else "INTEGER DEFAULT 0",
            )
            _ensure_column(
                conn,
                "submission_scores",
                "show_feedback_to_learner",
                "BOOLEAN DEFAULT FALSE" if RUNTIME_USE_POSTGRES else "INTEGER DEFAULT 0",
            )
            _ensure_column(
                conn,
                "submission_scores",
                "show_expected_answers_to_learner",
                "BOOLEAN DEFAULT FALSE" if RUNTIME_USE_POSTGRES else "INTEGER DEFAULT 0",
            )
            _ensure_column(
                conn,
                "submission_scores",
                "show_grading_criteria_to_learner",
                "BOOLEAN DEFAULT FALSE" if RUNTIME_USE_POSTGRES else "INTEGER DEFAULT 0",
            )
            _ensure_column(
                conn,
                "submission_scores",
                "show_ai_evaluation_details_to_learner",
                "BOOLEAN DEFAULT FALSE" if RUNTIME_USE_POSTGRES else "INTEGER DEFAULT 0",
            )
            _ensure_column(
                conn,
                "submission_scores",
                "show_ai_review_to_learner",
                "BOOLEAN DEFAULT FALSE" if RUNTIME_USE_POSTGRES else "INTEGER DEFAULT 0",
            )
            _ensure_column(
                conn,
                "submission_scores",
                "show_learner_responses_to_learner",
                "BOOLEAN DEFAULT FALSE" if RUNTIME_USE_POSTGRES else "INTEGER DEFAULT 0",
            )
            _ensure_column(
                conn,
                "submission_scores",
                "results_visibility_json",
                "JSONB" if RUNTIME_USE_POSTGRES else "TEXT",
            )
            if RUNTIME_USE_POSTGRES:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        ALTER TABLE submission_scores
                        ADD COLUMN IF NOT EXISTS show_results_to_learner BOOLEAN DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS show_overall_score_to_learner BOOLEAN DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS show_question_scores_to_learner BOOLEAN DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS show_feedback_to_learner BOOLEAN DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS show_expected_answers_to_learner BOOLEAN DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS show_ai_review_to_learner BOOLEAN DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS show_grading_criteria_to_learner BOOLEAN DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS show_learner_responses_to_learner BOOLEAN DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS results_visibility_json JSONB DEFAULT '{}'::jsonb
                        """
                    )
                    cur.execute(
                        """
                        ALTER TABLE submission_scores
                        ALTER COLUMN results_visibility_json
                        TYPE JSONB
                        USING CASE
                            WHEN COALESCE(BTRIM(results_visibility_json::text), '') = '' THEN '{}'::jsonb
                            ELSE results_visibility_json::jsonb
                        END
                        """
                    )
            _ensure_column(conn, "submission_scores", "approved_by", "BIGINT")
            _ensure_column(conn, "submission_scores", "approved_at", "TIMESTAMPTZ")
            _ensure_column(conn, "submission_scores", "scoring_method", "TEXT DEFAULT 'keyword'")
            _ensure_column(conn, "submission_scores", "scoring_breakdown_json", "TEXT")
            _ensure_column(conn, "submission_scores", "ai_reasoning_json", "TEXT")
            _ensure_column(conn, "submission_scores", "grading_error", "TEXT")
            if RUNTIME_USE_POSTGRES:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE submission_scores
                        SET review_status = CASE
                            WHEN LOWER(BTRIM(COALESCE(review_status, ''))) IN ('submitted', 'pending_review', 'approved')
                                THEN LOWER(BTRIM(review_status))
                            WHEN LOWER(BTRIM(COALESCE(grading_status, ''))) = 'approved'
                                THEN 'approved'
                            WHEN LOWER(BTRIM(COALESCE(grading_status, ''))) IN ('pending_review', 'ai_graded_pending_review', 'ai_grading', 'returned', 'grading_failed')
                                THEN 'pending_review'
                            ELSE 'submitted'
                        END
                        """
                    )
                    cur.execute(
                        """
                        UPDATE submission_scores
                        SET show_ai_review_to_learner = COALESCE(show_ai_evaluation_details_to_learner, FALSE)
                        WHERE COALESCE(show_ai_review_to_learner, FALSE) = FALSE
                          AND COALESCE(show_ai_evaluation_details_to_learner, FALSE) = TRUE
                        """
                    )
                    cur.execute(
                        """
                        UPDATE submission_scores
                        SET show_results_to_learner = TRUE,
                            show_overall_score_to_learner = TRUE,
                            show_question_scores_to_learner = TRUE,
                            show_feedback_to_learner = TRUE,
                            show_expected_answers_to_learner = TRUE,
                            show_grading_criteria_to_learner = TRUE,
                            show_ai_evaluation_details_to_learner = TRUE,
                            show_ai_review_to_learner = TRUE
                        WHERE review_status = 'approved'
                          AND COALESCE(show_results_to_learner, FALSE) = FALSE
                          AND COALESCE(show_overall_score_to_learner, FALSE) = FALSE
                          AND COALESCE(show_question_scores_to_learner, FALSE) = FALSE
                          AND COALESCE(show_feedback_to_learner, FALSE) = FALSE
                          AND COALESCE(show_expected_answers_to_learner, FALSE) = FALSE
                          AND COALESCE(show_grading_criteria_to_learner, FALSE) = FALSE
                          AND COALESCE(show_ai_evaluation_details_to_learner, FALSE) = FALSE
                        """
                    )
                    cur.execute(
                        """
                        UPDATE submission_scores
                        SET results_visibility_json = jsonb_build_object(
                            'show_results_to_learner', COALESCE(submission_scores.show_results_to_learner, FALSE),
                            'show_overall_score_to_learner', COALESCE(submission_scores.show_overall_score_to_learner, FALSE),
                            'show_question_scores_to_learner', COALESCE(submission_scores.show_question_scores_to_learner, FALSE),
                            'show_feedback_to_learner', COALESCE(submission_scores.show_feedback_to_learner, FALSE),
                            'show_expected_answers_to_learner', COALESCE(submission_scores.show_expected_answers_to_learner, FALSE),
                            'show_grading_criteria_to_learner', COALESCE(submission_scores.show_grading_criteria_to_learner, FALSE),
                            'show_ai_review_to_learner', COALESCE(submission_scores.show_ai_review_to_learner, submission_scores.show_ai_evaluation_details_to_learner, FALSE),
                            'show_learner_responses_to_learner', TRUE
                        )
                        FROM attempts a
                        JOIN modules m ON m.module_id = a.module_id
                        WHERE submission_scores.attempt_id = a.attempt_id
                          AND COALESCE(submission_scores.results_visibility_json, '{}'::jsonb) = '{}'::jsonb
                        """
                    )
                    cur.execute(
                        """
                        UPDATE submission_scores
                        SET show_feedback_to_learner = TRUE
                        FROM attempts a
                        JOIN modules m ON m.module_id = a.module_id
                        WHERE submission_scores.attempt_id = a.attempt_id
                          AND COALESCE(submission_scores.show_feedback_to_learner, FALSE) = FALSE
                          AND LOWER(BTRIM(COALESCE(m.learner_feedback_visibility, 'admin_approved_only'))) = 'always_show_ai_feedback'
                        """
                    )
            else:
                conn.execute(
                    """
                    UPDATE submission_scores
                    SET review_status = CASE
                        WHEN LOWER(TRIM(COALESCE(review_status, ''))) IN ('submitted', 'pending_review', 'approved')
                            THEN LOWER(TRIM(review_status))
                        WHEN LOWER(TRIM(COALESCE(grading_status, ''))) = 'approved'
                            THEN 'approved'
                        WHEN LOWER(TRIM(COALESCE(grading_status, ''))) IN ('pending_review', 'ai_graded_pending_review', 'ai_grading', 'returned', 'grading_failed')
                            THEN 'pending_review'
                        ELSE 'submitted'
                    END
                    """
                )
                conn.execute(
                    """
                    UPDATE submission_scores
                    SET show_feedback_to_learner = 1
                    WHERE COALESCE(show_feedback_to_learner, 0) = 0
                      AND attempt_id IN (
                          SELECT a.attempt_id
                          FROM attempts a
                          JOIN modules m ON m.module_id = a.module_id
                          WHERE LOWER(TRIM(COALESCE(m.learner_feedback_visibility, 'admin_approved_only'))) = 'always_show_ai_feedback'
                      )
                    """
                )
                conn.execute(
                    """
                    UPDATE submission_scores
                    SET show_ai_review_to_learner = COALESCE(show_ai_evaluation_details_to_learner, 0)
                    WHERE COALESCE(show_ai_review_to_learner, 0) = 0
                      AND COALESCE(show_ai_evaluation_details_to_learner, 0) = 1
                    """
                )
                conn.execute(
                    """
                    UPDATE submission_scores
                    SET results_visibility_json = json_object(
                        'show_results_to_learner', COALESCE(show_results_to_learner, 0),
                        'show_overall_score_to_learner', COALESCE(show_overall_score_to_learner, 0),
                        'show_question_scores_to_learner', COALESCE(show_question_scores_to_learner, 0),
                        'show_feedback_to_learner', COALESCE(show_feedback_to_learner, 0),
                        'show_expected_answers_to_learner', COALESCE(show_expected_answers_to_learner, 0),
                        'show_grading_criteria_to_learner', COALESCE(show_grading_criteria_to_learner, 0),
                        'show_ai_review_to_learner', COALESCE(show_ai_review_to_learner, show_ai_evaluation_details_to_learner, 0),
                        'show_learner_responses_to_learner', COALESCE(show_learner_responses_to_learner, 0)
                    )
                    WHERE COALESCE(TRIM(results_visibility_json), '') = ''
                    """
                )
                conn.execute(
                    """
                    UPDATE submission_scores
                    SET show_results_to_learner = 1,
                        show_overall_score_to_learner = 1,
                        show_question_scores_to_learner = 1,
                        show_feedback_to_learner = 1,
                        show_expected_answers_to_learner = 1,
                        show_grading_criteria_to_learner = 1,
                        show_ai_evaluation_details_to_learner = 1,
                        show_ai_review_to_learner = 1
                    WHERE review_status = 'approved'
                      AND COALESCE(show_results_to_learner, 0) = 0
                      AND COALESCE(show_overall_score_to_learner, 0) = 0
                      AND COALESCE(show_question_scores_to_learner, 0) = 0
                      AND COALESCE(show_feedback_to_learner, 0) = 0
                      AND COALESCE(show_expected_answers_to_learner, 0) = 0
                      AND COALESCE(show_grading_criteria_to_learner, 0) = 0
                      AND COALESCE(show_ai_evaluation_details_to_learner, 0) = 0
                    """
                )
            _ensure_column(conn, "module_generation_runs", "generation_status", "TEXT DEFAULT 'pending'")
            if RUNTIME_USE_POSTGRES:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE module_generation_runs
                        SET generation_status = 'pending'
                        WHERE LOWER(BTRIM(COALESCE(generation_status, ''))) = 'draft'
                        """
                    )
                    cur.execute(
                        "ALTER TABLE module_generation_runs ALTER COLUMN generation_status SET DEFAULT 'pending'"
                    )
            else:
                conn.execute(
                    """
                    UPDATE module_generation_runs
                    SET generation_status = 'pending'
                    WHERE LOWER(TRIM(COALESCE(generation_status, ''))) = 'draft'
                    """
                )
            _ensure_column(conn, "module_generation_runs", "input_content_sections", "TEXT")
            _ensure_column(
                conn,
                "module_generation_runs",
                "input_quiz_required",
                "BOOLEAN DEFAULT FALSE" if RUNTIME_USE_POSTGRES else "INTEGER DEFAULT 0",
            )
            _ensure_column(conn, "module_generation_runs", "input_estimated_minutes", "INTEGER")
            if RUNTIME_USE_POSTGRES:
                _migrate_input_quiz_required_to_boolean(conn)
                _migrate_submitted_state_to_integer(conn)
            _ensure_column(conn, "module_generation_questions", "approval_status", "TEXT DEFAULT 'pending'")
            _ensure_column(conn, "module_generation_questions", "question_type", "TEXT DEFAULT 'open_text'")
            _ensure_column(conn, "module_generation_questions", "options_text", "TEXT")
            _ensure_column(conn, "module_questions", "question_type", "TEXT DEFAULT 'open_text'")
            _ensure_column(conn, "module_questions", "options_text", "TEXT")
            _ensure_column(conn, "module_questions", "expected_answer", "TEXT")
            _ensure_column(conn, "module_questions", "rubric", "TEXT")
            _ensure_column(conn, "module_questions", "partial_credit_guidance", "TEXT")
            _ensure_column(conn, "module_questions", "incorrect_criteria", "TEXT")
            _ensure_column(conn, "module_questions", "incomplete_criteria", "TEXT")
            _ensure_column(conn, "module_questions", "strong_response_criteria", "TEXT")
            _ensure_column(conn, "module_questions", "max_points", "DOUBLE PRECISION DEFAULT 10")
            _ensure_column(conn, "module_questions", "scoring_type", "TEXT DEFAULT 'manual'")
            _ensure_column(conn, "module_questions", "scoring_style", "TEXT")
            _ensure_column(conn, "module_questions", "keyword_expected_terms", "TEXT")
            _ensure_column(conn, "module_questions", "llm_grading_criteria", "TEXT")
            _ensure_column(conn, "module_questions", "llm_grading_instructions", "TEXT")
            _ensure_column(conn, "module_questions", "learner_visible_feedback_mode", "TEXT DEFAULT 'admin_approved_only'")
            _ensure_column(conn, "module_questions", "rubric_criteria_json", "TEXT")
            _ensure_column(conn, "module_questions", "ai_conversation_prompt", "TEXT")
            _ensure_column(conn, "module_questions", "ai_role_or_persona", "TEXT")
            _ensure_column(conn, "module_questions", "evaluation_focus", "TEXT")
            _ensure_column(conn, "module_questions", "max_learner_responses", "INTEGER DEFAULT 3")
            _ensure_column(conn, "module_questions", "optional_wrap_up_instruction", "TEXT")
            _ensure_column(conn, "module_questions", "wrap_up_message_optional", "TEXT")
            _ensure_column(conn, "modules", "llm_scoring_enabled", "BOOLEAN DEFAULT FALSE")
            _ensure_column(conn, "modules", "scoring_style", "TEXT DEFAULT 'keyword'")
            _ensure_column(conn, "modules", "llm_grader_instructions", "TEXT")
            _ensure_column(conn, "modules", "learner_feedback_visibility", "TEXT DEFAULT 'admin_approved_only'")
            _ensure_column(conn, "modules", "scoring_config_json", "TEXT")
            if RUNTIME_USE_POSTGRES:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE module_questions
                        SET scoring_type = CASE
                            WHEN LOWER(COALESCE(scoring_type, '')) IN ('manual', 'keyword', 'llm') THEN LOWER(scoring_type)
                            WHEN LOWER(COALESCE(scoring_style, '')) = 'manual_review' THEN 'manual'
                            WHEN LOWER(COALESCE(scoring_style, '')) IN ('llm', 'rubric_llm', 'llm_rubric') THEN 'llm'
                            WHEN LOWER(COALESCE(scoring_style, '')) = 'keyword' THEN 'keyword'
                            ELSE 'manual'
                        END
                        WHERE COALESCE(scoring_type, '') = ''
                        """
                    )
            else:
                conn.execute(
                    """
                    UPDATE module_questions
                    SET scoring_type = CASE
                        WHEN LOWER(COALESCE(scoring_type, '')) IN ('manual', 'keyword', 'llm') THEN LOWER(scoring_type)
                        WHEN LOWER(COALESCE(scoring_style, '')) = 'manual_review' THEN 'manual'
                        WHEN LOWER(COALESCE(scoring_style, '')) IN ('llm', 'rubric_llm', 'llm_rubric') THEN 'llm'
                        WHEN LOWER(COALESCE(scoring_style, '')) = 'keyword' THEN 'keyword'
                        ELSE 'manual'
                    END
                    WHERE COALESCE(scoring_type, '') = ''
                    """
                )
            _ensure_column(conn, "assignment_workspace_state", "time_limit_minutes", "INTEGER")
            _ensure_column(conn, "assignment_workspace_state", "end_time", "TEXT")
            _ensure_column(conn, "assignment_workspace_state", "auto_submitted_state", "INTEGER DEFAULT 0")
            ensure_question_conversation_messages_table(conn)
            if RUNTIME_USE_POSTGRES:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS submission_question_scores (
                            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            attempt_id BIGINT NOT NULL,
                            question_id BIGINT,
                            learner_answer TEXT,
                            ai_awarded_points DOUBLE PRECISION,
                            ai_max_points DOUBLE PRECISION,
                            ai_feedback TEXT,
                            ai_reasoning TEXT,
                            missing_key_concepts TEXT,
                            admin_awarded_points DOUBLE PRECISION,
                            admin_feedback TEXT,
                            final_awarded_points DOUBLE PRECISION,
                            visible_to_learner BOOLEAN DEFAULT FALSE,
                            is_admin_override BOOLEAN DEFAULT FALSE,
                            created_at TIMESTAMPTZ DEFAULT NOW(),
                            updated_at TIMESTAMPTZ DEFAULT NOW(),
                            UNIQUE(attempt_id, question_id),
                            FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                            FOREIGN KEY(question_id) REFERENCES module_questions(question_id) ON DELETE SET NULL
                        )
                        """
                    )
            else:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS submission_question_scores (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        attempt_id INTEGER NOT NULL,
                        question_id INTEGER,
                        learner_answer TEXT,
                        ai_awarded_points REAL,
                        ai_max_points REAL,
                        ai_feedback TEXT,
                        ai_reasoning TEXT,
                        missing_key_concepts TEXT,
                        admin_awarded_points REAL,
                        admin_feedback TEXT,
                        final_awarded_points REAL,
                        visible_to_learner INTEGER DEFAULT 0,
                        is_admin_override INTEGER DEFAULT 0,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(attempt_id, question_id),
                        FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                        FOREIGN KEY(question_id) REFERENCES module_questions(question_id) ON DELETE SET NULL
                    )
                    """
                )
            _ensure_column(conn, "submission_question_scores", "ai_score", "DOUBLE PRECISION")
            _ensure_column(conn, "submission_question_scores", "admin_score", "DOUBLE PRECISION")
            _ensure_column(conn, "submission_question_scores", "final_score", "DOUBLE PRECISION")
            _ensure_column(conn, "submission_question_scores", "feedback", "TEXT")
            _ensure_column(conn, "submission_question_scores", "scoring_method", "TEXT")
            _ensure_column(conn, "submission_question_scores", "score_breakdown_json", "TEXT")
            _ensure_column(conn, "submission_question_scores", "conversation_transcript", "TEXT")

            if RUNTIME_USE_POSTGRES:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS module_rubric_criteria (
                            criteria_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            module_id BIGINT NOT NULL,
                            question_id BIGINT,
                            criterion_order INTEGER NOT NULL DEFAULT 1,
                            label TEXT NOT NULL,
                            description TEXT,
                            weight DOUBLE PRECISION DEFAULT 1,
                            max_points DOUBLE PRECISION DEFAULT 1,
                            grading_guidance TEXT,
                            is_active BOOLEAN DEFAULT TRUE,
                            created_at TIMESTAMPTZ DEFAULT NOW(),
                            updated_at TIMESTAMPTZ DEFAULT NOW(),
                            FOREIGN KEY(module_id) REFERENCES modules(module_id) ON DELETE CASCADE,
                            FOREIGN KEY(question_id) REFERENCES module_questions(question_id) ON DELETE CASCADE
                        )
                        """
                    )
            else:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS module_rubric_criteria (
                        criteria_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        module_id INTEGER NOT NULL,
                        question_id INTEGER,
                        criterion_order INTEGER NOT NULL DEFAULT 1,
                        label TEXT NOT NULL,
                        description TEXT,
                        weight REAL DEFAULT 1,
                        max_points REAL DEFAULT 1,
                        grading_guidance TEXT,
                        is_active INTEGER DEFAULT 1,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(module_id) REFERENCES modules(module_id) ON DELETE CASCADE,
                        FOREIGN KEY(question_id) REFERENCES module_questions(question_id) ON DELETE CASCADE
                    )
                    """
                )
            ensure_module_rubric_criteria_table(conn)

            if RUNTIME_USE_POSTGRES:
                _executescript(
                    conn,
                    """
                    CREATE TABLE IF NOT EXISTS submission_regrade_history (
                        regrade_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        attempt_id BIGINT NOT NULL,
                        old_total_score DOUBLE PRECISION,
                        new_total_score DOUBLE PRECISION,
                        old_category_scores_json TEXT,
                        new_category_scores_json TEXT,
                        reason TEXT,
                        changed_by_type TEXT NOT NULL DEFAULT 'admin',
                        changed_by_user_id BIGINT,
                        changed_at TIMESTAMPTZ DEFAULT NOW(),
                        FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                        FOREIGN KEY(changed_by_user_id) REFERENCES users(user_id)
                    )
                    """,
                )
            else:
                _executescript(
                    conn,
                    """
                    CREATE TABLE IF NOT EXISTS submission_regrade_history (
                        regrade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        attempt_id INTEGER NOT NULL,
                        old_total_score REAL,
                        new_total_score REAL,
                        old_category_scores_json TEXT,
                        new_category_scores_json TEXT,
                        reason TEXT,
                        changed_by_type TEXT NOT NULL DEFAULT 'admin',
                        changed_by_user_id INTEGER,
                        changed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                        FOREIGN KEY(changed_by_user_id) REFERENCES users(user_id)
                    )
                    """,
                )

            if RUNTIME_USE_POSTGRES:
                with conn.cursor() as cur:
                    # IMPORTANT: keep idx_learner_profiles_user_id out of runtime init.
                    # Create it manually via one-time SQL migration to avoid startup lock contention.
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_learner_profiles_status ON learner_profiles(status)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_module_assignments_user_id ON module_assignments(user_id)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_module_assignments_module_id ON module_assignments(module_id)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_module_progress_user_id ON module_progress(user_id)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_module_progress_module_id ON module_progress(module_id)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_module_progress_completed_at ON module_progress(completed_at)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_submission_scores_attempt_id ON submission_scores(attempt_id)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_submission_scores_total_score ON submission_scores(total_score)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_attempts_result_status ON attempts(result_status)")
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_submission_regrade_history_attempt_id ON submission_regrade_history(attempt_id)"
                    )
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
                    cur.execute(
                        """
                        CREATE OR REPLACE FUNCTION reset_attempt_approval_on_regrade()
                        RETURNS TRIGGER AS $$
                        BEGIN
                            UPDATE attempts
                            SET result_status = 'ai_graded_pending_review',
                                result_approved_at = NULL,
                                result_approved_by_user_id = NULL
                            WHERE attempt_id = NEW.attempt_id
                              AND (
                                  COALESCE(result_status, 'submitted') <> 'ai_graded_pending_review'
                                  OR result_approved_at IS NOT NULL
                                  OR result_approved_by_user_id IS NOT NULL
                              );
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql;
                        """
                    )
                    cur.execute("DROP TRIGGER IF EXISTS trg_submission_scores_reset_approval ON submission_scores")
                    cur.execute(
                        """
                        CREATE TRIGGER trg_submission_scores_reset_approval
                        AFTER UPDATE OF understanding_score, investigation_score, solution_score, communication_score, total_score
                        ON submission_scores
                        FOR EACH ROW
                        WHEN (
                            COALESCE(OLD.understanding_score, -1) <> COALESCE(NEW.understanding_score, -1)
                            OR COALESCE(OLD.investigation_score, -1) <> COALESCE(NEW.investigation_score, -1)
                            OR COALESCE(OLD.solution_score, -1) <> COALESCE(NEW.solution_score, -1)
                            OR COALESCE(OLD.communication_score, -1) <> COALESCE(NEW.communication_score, -1)
                            OR COALESCE(OLD.total_score, -1) <> COALESCE(NEW.total_score, -1)
                        )
                        EXECUTE FUNCTION reset_attempt_approval_on_regrade()
                        """
                    )
            else:
                conn.executescript(
                    """
                CREATE INDEX IF NOT EXISTS idx_learner_profiles_status ON learner_profiles(status);
                CREATE INDEX IF NOT EXISTS idx_module_assignments_user_id ON module_assignments(user_id);
                CREATE INDEX IF NOT EXISTS idx_module_assignments_module_id ON module_assignments(module_id);
                CREATE INDEX IF NOT EXISTS idx_module_progress_user_id ON module_progress(user_id);
                CREATE INDEX IF NOT EXISTS idx_module_progress_module_id ON module_progress(module_id);
                CREATE INDEX IF NOT EXISTS idx_module_progress_completed_at ON module_progress(completed_at);
                CREATE INDEX IF NOT EXISTS idx_submission_scores_attempt_id ON submission_scores(attempt_id);
                CREATE INDEX IF NOT EXISTS idx_submission_scores_total_score ON submission_scores(total_score);
                CREATE INDEX IF NOT EXISTS idx_attempts_result_status ON attempts(result_status);
                CREATE INDEX IF NOT EXISTS idx_submission_regrade_history_attempt_id ON submission_regrade_history(attempt_id);

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

                DROP TRIGGER IF EXISTS trg_attempts_result_status_validate_insert;
                CREATE TRIGGER trg_attempts_result_status_validate_insert
                BEFORE INSERT ON attempts
                FOR EACH ROW
                WHEN NEW.result_status IS NOT NULL
                  AND LOWER(TRIM(NEW.result_status)) NOT IN ('submitted', 'ai_grading', 'ai_graded_pending_review', 'pending_review', 'approved', 'returned', 'grading_failed')
                BEGIN
                    SELECT RAISE(ABORT, 'invalid attempts.result_status');
                END;

                DROP TRIGGER IF EXISTS trg_attempts_result_status_validate_update;
                CREATE TRIGGER trg_attempts_result_status_validate_update
                BEFORE UPDATE OF result_status ON attempts
                FOR EACH ROW
                WHEN NEW.result_status IS NOT NULL
                  AND LOWER(TRIM(NEW.result_status)) NOT IN ('submitted', 'ai_grading', 'ai_graded_pending_review', 'pending_review', 'approved', 'returned', 'grading_failed')
                BEGIN
                    SELECT RAISE(ABORT, 'invalid attempts.result_status');
                END;

                DROP TRIGGER IF EXISTS trg_submission_scores_reset_approval;
                CREATE TRIGGER trg_submission_scores_reset_approval
                AFTER UPDATE OF understanding_score, investigation_score, solution_score, communication_score, total_score
                ON submission_scores
                FOR EACH ROW
                WHEN (
                    COALESCE(OLD.understanding_score, -1) <> COALESCE(NEW.understanding_score, -1)
                    OR COALESCE(OLD.investigation_score, -1) <> COALESCE(NEW.investigation_score, -1)
                    OR COALESCE(OLD.solution_score, -1) <> COALESCE(NEW.solution_score, -1)
                    OR COALESCE(OLD.communication_score, -1) <> COALESCE(NEW.communication_score, -1)
                    OR COALESCE(OLD.total_score, -1) <> COALESCE(NEW.total_score, -1)
                )
                BEGIN
                    UPDATE attempts
                    SET result_status = 'ai_graded_pending_review',
                        result_approved_at = NULL,
                        result_approved_by_user_id = NULL
                    WHERE attempt_id = NEW.attempt_id
                      AND (
                        COALESCE(result_status, 'submitted') <> 'ai_graded_pending_review'
                        OR result_approved_at IS NOT NULL
                        OR result_approved_by_user_id IS NOT NULL
                      );
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
        finally:
            if lock_acquired and RUNTIME_USE_POSTGRES:
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s);", (123456,))
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
                    translated_query = _sql(query)
                    cur.execute(translated_query, params_tuple)
                    is_insert = translated_query.lstrip().upper().startswith("INSERT")
                    has_returning = "RETURNING" in translated_query.upper()
                    if is_insert and has_returning:
                        lastrow = cur.fetchone()
                        lastrowid = int(lastrow["id"]) if lastrow and "id" in lastrow else 0
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


def execute_update(
    table_name: str,
    updates: Dict[str, Any],
    where_clause: str,
    where_params: Iterable[Any] = (),
) -> None:
    if not updates:
        raise ValueError("updates must contain at least one column.")
    if not _is_safe_identifier(table_name):
        raise ValueError(f"Unsafe table name: {table_name}")

    invalid_columns = [column for column in updates.keys() if not _is_safe_identifier(column)]
    if invalid_columns:
        raise ValueError(f"Unsafe update column(s): {', '.join(sorted(invalid_columns))}")

    set_clause = ", ".join(f"{column} = ?" for column in updates.keys())
    query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
    execute(query, tuple(updates.values()) + tuple(where_params))


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
    timed_out_value = 1 if bool(payload.get("timed_out")) else 0

    category_scores = payload.get("category_scores") or {}
    has_legacy_scores = isinstance(category_scores, dict) and bool(category_scores)
    payload_result_status = str(payload.get("result_status", "submitted") or "submitted").strip().lower()
    review_status_value = "approved" if payload_result_status == "approved" else ("pending_review" if payload_result_status in {"pending_review", "ai_grading", "ai_graded_pending_review", "returned", "grading_failed"} else "submitted")
    attempt_id = execute(
        """
        INSERT INTO attempts (
            user_id, module_id, organization_id, started_at, submitted_at, elapsed_seconds, time_limit_seconds, time_remaining_seconds, attempt_state, graded_by_type, graded_by_user_id, graded_at,
            result_status, result_approved_at, result_approved_by_user_id,
            diagnosis_answer, next_steps_answer, customer_response, escalation_choice, notes, timed_out, question_responses,
            understanding_score, investigation_score, solution_score, communication_score,
            total_score, ai_feedback, strengths, missed_points,
            best_practice_reasoning, recommended_response, takeaway_summary
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING attempt_id AS id
        """,
        (
            user_id,
            module_id,
            organization_id,
            payload.get("started_at"),
            payload.get("submitted_at"),
            payload.get("elapsed_seconds"),
            payload.get("time_limit_seconds"),
            payload.get("time_remaining_seconds"),
            payload.get("attempt_state", "graded"),
            payload.get("graded_by_type", "system"),
            payload.get("graded_by_user_id"),
            payload.get("graded_at", payload.get("submitted_at")),
            payload_result_status,
            None,
            None,
            payload.get("diagnosis_answer"),
            payload.get("next_steps_answer"),
            payload.get("customer_response"),
            payload.get("escalation_choice"),
            payload.get("notes"),
            timed_out_value,
            payload.get("question_responses"),
            category_scores.get("understanding"),
            category_scores.get("investigation"),
            category_scores.get("solution_quality"),
            category_scores.get("communication"),
            payload.get("total_score"),
            payload.get("coaching_feedback"),
            json.dumps(payload.get("strengths", [])),
            json.dumps(payload.get("missed_points", [])),
            payload.get("best_practice_reasoning"),
            payload.get("recommended_response"),
            payload.get("takeaway_summary"),
        ),
    )

    if has_legacy_scores:
        execute(
            """
            INSERT INTO submission_scores (
                attempt_id,
                scoring_version,
                understanding_score,
                investigation_score,
                solution_score,
                communication_score,
                understanding_rationale,
                investigation_rationale,
                solution_rationale,
                communication_rationale,
                total_score,
                scoring_provider,
                scoring_model_name,
                scoring_prompt_template_id,
                scoring_temperature,
                scoring_config_json,
                score_inputs_json,
                grading_status,
                review_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(attempt_id) DO UPDATE SET
                understanding_score = excluded.understanding_score,
                investigation_score = excluded.investigation_score,
                solution_score = excluded.solution_score,
                communication_score = excluded.communication_score,
                total_score = excluded.total_score,
                grading_status = excluded.grading_status,
                review_status = excluded.review_status
            """,
            (
                attempt_id,
                payload.get("scoring_version", "heuristic_v1"),
                category_scores.get("understanding"),
                category_scores.get("investigation"),
                category_scores.get("solution_quality"),
                category_scores.get("communication"),
                payload.get("category_rationales", {}).get("understanding"),
                payload.get("category_rationales", {}).get("investigation"),
                payload.get("category_rationales", {}).get("solution_quality"),
                payload.get("category_rationales", {}).get("communication"),
                payload.get("total_score"),
                payload.get("scoring_engine", {}).get("provider"),
                payload.get("scoring_engine", {}).get("model_name"),
                payload.get("scoring_engine", {}).get("prompt_template_id"),
                payload.get("scoring_engine", {}).get("temperature"),
                json.dumps(payload.get("scoring_engine", {}).get("config", {})),
                json.dumps(
                    {
                        "actions_used": payload.get("actions_used", []),
                        "actions_used_count": payload.get("actions_used_count", 0),
                        "escalation_choice": payload.get("escalation_choice"),
                    }
                ),
                payload_result_status,
                review_status_value,
            ),
        )
    else:
        execute(
            """
            INSERT INTO submission_scores (attempt_id, grading_status, review_status, score_inputs_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(attempt_id) DO UPDATE SET
                grading_status = excluded.grading_status,
                review_status = excluded.review_status,
                score_inputs_json = excluded.score_inputs_json
            """,
            (
                attempt_id,
                payload_result_status,
                review_status_value,
                json.dumps(
                    {
                        "actions_used": payload.get("actions_used", []),
                        "actions_used_count": payload.get("actions_used_count", 0),
                        "escalation_choice": payload.get("escalation_choice"),
                    }
                ),
            ),
        )
    return attempt_id


def log_actions(attempt_id: int, actions: List[str]) -> None:
    if not actions:
        return
    executemany(
        "INSERT INTO action_logs (attempt_id, action_name) VALUES (?, ?)",
        [(attempt_id, action) for action in actions],
    )


def record_regrade(
    attempt_id: int,
    old_scores: Dict[str, Any],
    new_scores: Dict[str, Any],
    reason: str,
    changed_by_type: str = "admin",
    changed_by_user_id: int | None = None,
) -> None:
    execute(
        """
        INSERT INTO submission_regrade_history (
            attempt_id,
            old_total_score,
            new_total_score,
            old_category_scores_json,
            new_category_scores_json,
            reason,
            changed_by_type,
            changed_by_user_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            attempt_id,
            old_scores.get("total_score"),
            new_scores.get("total_score"),
            json.dumps(old_scores.get("category_scores", {})),
            json.dumps(new_scores.get("category_scores", {})),
            reason,
            changed_by_type,
            changed_by_user_id,
        ),
    )
