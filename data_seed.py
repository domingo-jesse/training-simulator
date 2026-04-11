from db import RUNTIME_USE_POSTGRES, execute, executemany, fetch_all, fetch_one

DEFAULT_ORG = "Acme Health"
SEED_EMAIL_DOMAIN = "@acmehealth.example"
SEED_MODULE_TITLES = (
    "PA Denial Spike in Orthopedics",
    "Bot Login Failures After Credential Rotation",
    "Portal Workflow Update Broke Intake Routing",
)


def clear_seed_data() -> None:
    module_ids = [int(row["module_id"]) for row in fetch_all("SELECT module_id FROM modules WHERE title IN (?, ?, ?)", SEED_MODULE_TITLES)]
    if module_ids:
        placeholders = ",".join(["?"] * len(module_ids))
        execute(f"DELETE FROM action_logs WHERE attempt_id IN (SELECT attempt_id FROM attempts WHERE module_id IN ({placeholders}))", tuple(module_ids))
        execute(f"DELETE FROM attempts WHERE module_id IN ({placeholders})", tuple(module_ids))
        execute(f"DELETE FROM assignments WHERE module_id IN ({placeholders})", tuple(module_ids))
        execute(f"DELETE FROM module_assignments WHERE module_id IN ({placeholders})", tuple(module_ids))
        execute(f"DELETE FROM module_progress WHERE module_id IN ({placeholders})", tuple(module_ids))
        execute(f"DELETE FROM investigation_actions WHERE module_id IN ({placeholders})", tuple(module_ids))
        execute(f"DELETE FROM modules WHERE module_id IN ({placeholders})", tuple(module_ids))

    seed_users = [int(row["user_id"]) for row in fetch_all("SELECT user_id FROM users WHERE email LIKE ?", (f"%{SEED_EMAIL_DOMAIN}",))]
    if seed_users:
        user_placeholders = ",".join(["?"] * len(seed_users))
        execute(f"DELETE FROM action_logs WHERE user_id IN ({user_placeholders})", tuple(seed_users))
        execute(f"DELETE FROM attempts WHERE user_id IN ({user_placeholders})", tuple(seed_users))
        execute(f"DELETE FROM assignments WHERE learner_id IN ({user_placeholders}) OR assigned_by IN ({user_placeholders})", tuple(seed_users + seed_users))
        execute(f"DELETE FROM module_assignments WHERE user_id IN ({user_placeholders})", tuple(seed_users))
        execute(f"DELETE FROM learner_profiles WHERE user_id IN ({user_placeholders})", tuple(seed_users))
        execute(f"DELETE FROM module_progress WHERE user_id IN ({user_placeholders})", tuple(seed_users))
        execute(f"DELETE FROM users WHERE user_id IN ({user_placeholders})", tuple(seed_users))

    execute(
        """
        DELETE FROM organizations
        WHERE name = ?
          AND organization_id NOT IN (SELECT DISTINCT organization_id FROM users WHERE organization_id IS NOT NULL)
          AND organization_id NOT IN (SELECT DISTINCT organization_id FROM modules WHERE organization_id IS NOT NULL)
          AND organization_id NOT IN (SELECT DISTINCT organization_id FROM attempts WHERE organization_id IS NOT NULL)
          AND organization_id NOT IN (SELECT DISTINCT organization_id FROM assignments WHERE organization_id IS NOT NULL)
        """,
        (DEFAULT_ORG,),
    )


def _ensure_org() -> int:
    org = fetch_one("SELECT organization_id FROM organizations WHERE name = ?", (DEFAULT_ORG,))
    if org:
        return int(org["organization_id"])
    return int(execute("INSERT INTO organizations (name) VALUES (?)", (DEFAULT_ORG,)))


def _ensure_user(name: str, email: str, role: str, team: str, org_id: int, is_active: int = 1) -> int:
    existing = fetch_one("SELECT user_id FROM users WHERE LOWER(email) = ?", (email.strip().lower(),))
    if existing:
        execute(
            """
            UPDATE users
            SET name = ?, role = ?, team = ?, organization_id = COALESCE(organization_id, ?), is_active = ?
            WHERE user_id = ?
            """,
            (name, role, team, org_id, is_active, int(existing["user_id"])),
        )
        return int(existing["user_id"])

    return int(
        execute(
            "INSERT INTO users (name, email, role, team, organization_id, is_active) VALUES (?, ?, ?, ?, ?, ?)",
            (name, email.strip().lower(), role, team, org_id, is_active),
        )
    )


def seed_users() -> dict[str, int]:
    org_id = _ensure_org()
    user_specs = [
        ("Ava Patel", "ava.patel@acmehealth.example", "learner", "Operations", 1),
        ("Jordan Lee", "jordan.lee@acmehealth.example", "learner", "Revenue Cycle", 1),
        ("Sam Rivera", "sam.rivera@acmehealth.example", "learner", "Platform Support", 1),
        ("Mia Chen", "mia.chen@acmehealth.example", "learner", "Clinical Ops", 1),
        ("Noah Brooks", "noah.brooks@acmehealth.example", "learner", "Provider Success", 1),
        ("Priya Singh", "priya.singh@acmehealth.example", "learner", "Member Support", 1),
        ("Admin User", "admin@acmehealth.example", "admin", "Training", 1),
        ("Taylor Admin", "taylor.admin@acmehealth.example", "admin", "Enablement", 1),
    ]

    user_ids: dict[str, int] = {}
    for name, email, role, team, is_active in user_specs:
        user_ids[email] = _ensure_user(name, email, role, team, org_id, is_active)
    return user_ids


def seed_modules() -> None:
    if fetch_one("SELECT module_id FROM modules LIMIT 1"):
        return

    org_id = _ensure_org()

    modules = [
        (
            "PA Denial Spike in Orthopedics",
            "Prior Authorization",
            "Intermediate",
            "Claims suddenly rejected for valid PA numbers in one specialty queue.",
            "20 min",
            "Ticket #PA-4412: 37 prior auth submissions denied with code INVALID_AUTH over 2 hours.",
            "Issue started right after yesterday's payer mapping update. Frontline staff verified patient IDs are correct.",
            "Payer ID mapping table points to legacy endpoint for one plan variant.",
            "Validate denial pattern by payer and plan; confirm timing with deployment; compare mapping config across envs.",
            "Denials are caused by outdated payer mapping for a specific plan variant introduced in the latest config deploy.",
            "Rollback or hotfix mapping, requeue affected submissions, add pre-deploy mapping validation checks.",
            "Acknowledge impact, confirm identified config mismatch, provide ETA for replay and prevention actions.",
            "Configuration changes in payer integrations need plan-level validation and post-deploy monitoring.",
            org_id,
            "published",
            "Identify relevant logs\nIsolate probable root cause\nCommunicate remediation",
            "Collect evidence\nValidate hypothesis\nClose the loop",
            "Submit diagnosis and next steps",
            0,
        ),
        (
            "Bot Login Failures After Credential Rotation",
            "Automation",
            "Beginner",
            "Nightly eligibility bot failed for 112 accounts after security rotation.",
            "15 min",
            "Ticket #AUT-2209: Automation run failed with AUTH_401 from 01:04 onward.",
            "Security team rotated service credentials at midnight. Bot owners claim vault integration is automated.",
            "Secret alias in vault updated, but bot uses old hardcoded secret key name.",
            "Confirm time of credential rotation, inspect auth logs, verify vault alias usage, test bot secret retrieval.",
            "Bot references stale secret key alias, so it cannot fetch newly rotated credentials.",
            "Update bot to use dynamic vault alias, trigger rerun, document runbook for rotations.",
            "Explain root cause in plain language, outline immediate fix and long-term reliability step.",
            "Credential automation should be alias-driven and validated with synthetic checks after rotations.",
            org_id,
            "published",
            "Validate authentication path\nIdentify stale credentials",
            "Check auth logs\nConfirm secret alias\nPropose fix",
            "Complete scenario submission",
            0,
        ),
        (
            "Portal Workflow Update Broke Intake Routing",
            "Workflow",
            "Advanced",
            "Referral portal submissions are landing in 'Unassigned' instead of nurse intake queue.",
            "25 min",
            "Ticket #WF-7781: Intake backlog grew by 63 after portal release v3.9.",
            "New optional field 'Urgency Tier' was added in portal form yesterday. Routing rules were not reviewed by ops.",
            "Workflow rule expects old field schema and fails when Urgency Tier is null.",
            "Review recent release notes, inspect routing rule conditions, compare payload schema, test with sample submission.",
            "Routing rule logic is incompatible with new form schema, causing fallback to Unassigned.",
            "Patch routing conditions for new field, backfill queue assignments, add schema compatibility tests.",
            "Share impact, reassure no data loss, provide remediation timeline and monitoring commitment.",
            "Workflow releases need schema contract testing and coordinated change review with operations.",
            org_id,
            "published",
            "Analyze workflow rules\nAccount for schema changes\nPlan preventive tests",
            "Inspect release\nCompare schema\nPatch rule",
            "Submit root cause + prevention",
            1,
        ),
    ]

    for module in modules:
        module_id = execute(
            """
            INSERT INTO modules (
                title, category, difficulty, description, estimated_time,
                scenario_ticket, scenario_context, hidden_root_cause,
                expected_reasoning_path, expected_diagnosis, expected_next_steps,
                expected_customer_response, lesson_takeaway, organization_id, status,
                learning_objectives, content_sections, completion_requirements, quiz_required
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            module,
        )

        actions = [
            (
                module_id,
                "Check logs",
                "Application logs show error spike beginning within 10 minutes of latest change window.",
            ),
            (
                module_id,
                "Review recent changes",
                "Release and config history shows a deployment in the impacted system before first reported failures.",
            ),
            (
                module_id,
                "Compare environments",
                "Staging behaves correctly; production has one divergent config key affecting request routing.",
            ),
            (
                module_id,
                "Ask for an example",
                "A recent failed case includes valid patient details but malformed integration metadata downstream.",
            ),
            (
                module_id,
                "Check credentials",
                "Service account shows successful token issue except in the failing automated workflow.",
            ),
            (
                module_id,
                "Review system status",
                "No platform outage detected. Incident appears isolated to one workflow path.",
            ),
        ]
        executemany(
            "INSERT INTO investigation_actions (module_id, action_name, revealed_information) VALUES (?, ?, ?)",
            actions,
        )


def seed_assignments_and_attempts(user_ids: dict[str, int]) -> None:
    if fetch_one("SELECT assignment_id FROM assignments LIMIT 1") or fetch_one("SELECT attempt_id FROM attempts LIMIT 1"):
        return

    org_id = _ensure_org()
    module_rows = fetch_all(
        "SELECT module_id, title, expected_diagnosis, expected_next_steps, expected_customer_response FROM modules WHERE organization_id = ? ORDER BY module_id",
        (org_id,),
    )
    module_map = {row["title"]: dict(row) for row in module_rows}

    admin_id = user_ids["admin@acmehealth.example"]
    learners = [
        user_ids["ava.patel@acmehealth.example"],
        user_ids["jordan.lee@acmehealth.example"],
        user_ids["sam.rivera@acmehealth.example"],
        user_ids["mia.chen@acmehealth.example"],
        user_ids["noah.brooks@acmehealth.example"],
        user_ids["priya.singh@acmehealth.example"],
    ]

    assignment_rows = [
        # Completed stage
        (org_id, module_map["PA Denial Spike in Orthopedics"]["module_id"], learners[0], admin_id, "2026-03-20", "2026-03-01 09:00:00", 1),
        # Completed with lower score (represents developing proficiency)
        (org_id, module_map["Bot Login Failures After Credential Rotation"]["module_id"], learners[1], admin_id, "2026-04-20", "2026-04-01 08:30:00", 1),
        # Overdue not started
        (org_id, module_map["Portal Workflow Update Broke Intake Routing"]["module_id"], learners[2], admin_id, "2026-04-01", "2026-03-18 10:00:00", 1),
        # Not started upcoming
        (org_id, module_map["PA Denial Spike in Orthopedics"]["module_id"], learners[3], admin_id, "2026-04-30", "2026-04-05 13:00:00", 1),
        # Completed with high score
        (org_id, module_map["Bot Login Failures After Credential Rotation"]["module_id"], learners[4], admin_id, "2026-04-12", "2026-03-28 11:15:00", 1),
        # Completed and due soon
        (org_id, module_map["Portal Workflow Update Broke Intake Routing"]["module_id"], learners[5], admin_id, "2026-04-10", "2026-04-02 14:20:00", 1),
    ]
    executemany(
        """
        INSERT INTO assignments (organization_id, module_id, learner_id, assigned_by, due_date, assigned_at, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        assignment_rows,
    )

    attempts = [
        (
            learners[0],
            module_map["PA Denial Spike in Orthopedics"]["module_id"],
            org_id,
            module_map["PA Denial Spike in Orthopedics"]["expected_diagnosis"],
            module_map["PA Denial Spike in Orthopedics"]["expected_next_steps"],
            module_map["PA Denial Spike in Orthopedics"]["expected_customer_response"],
            "Escalate to Product",
            "Confirmed plan-variant mismatch and rollback path.",
            92.0,
            90.0,
            88.0,
            91.0,
            90.3,
            "Strong triage and communication. Great evidence-based diagnosis.",
            '["Clear incident timeline", "Correct root-cause identification"]',
            '["Could include exact replay count earlier"]',
            "You validated the change window and isolated the config regression quickly.",
            "We found the issue in the payer mapping config and are replaying impacted claims today.",
            "Map validation checks should run pre- and post-deploy.",
            "2026-03-02 10:05:00",
        ),
        (
            learners[1],
            module_map["Bot Login Failures After Credential Rotation"]["module_id"],
            org_id,
            "Likely stale credential alias after rotation.",
            "Validate vault alias references and rerun the job.",
            "We identified a credential alias mismatch and are applying a fix now.",
            "Escalate to Engineering",
            "Need confirmation from security on alias policy.",
            72.0,
            70.0,
            68.0,
            74.0,
            71.0,
            "Good direction; add deeper verification of secret retrieval path.",
            '["Solid incident summary"]',
            '["Missing explicit synthetic test recommendation"]',
            "Focus on proving whether the bot is reading dynamic aliases at runtime.",
            "We're updating the bot to resolve aliases dynamically and validating with test runs.",
            "Credential rotations need resilient alias lookups and smoke tests.",
            "2026-04-01 09:15:00",
        ),
        (
            learners[4],
            module_map["Bot Login Failures After Credential Rotation"]["module_id"],
            org_id,
            module_map["Bot Login Failures After Credential Rotation"]["expected_diagnosis"],
            module_map["Bot Login Failures After Credential Rotation"]["expected_next_steps"],
            module_map["Bot Login Failures After Credential Rotation"]["expected_customer_response"],
            "No escalation",
            "Validated with auth logs and vault integration checks.",
            97.0,
            95.0,
            96.0,
            94.0,
            95.5,
            "Excellent root-cause proof and actionable remediation plan.",
            '["Complete evidence chain", "Proactive prevention mindset"]',
            '["Could tighten stakeholder ETA wording"]',
            "You connected security timeline, alias behavior, and bot implementation cleanly.",
            "The failed run is tied to a stale secret alias and has been corrected with dynamic alias retrieval.",
            "Runbooks should include post-rotation synthetic checks.",
            "2026-03-30 16:40:00",
        ),
        (
            learners[5],
            module_map["Portal Workflow Update Broke Intake Routing"]["module_id"],
            org_id,
            "Routing likely failed due to new Urgency Tier field handling.",
            "Update schema handling and backfill queue routing.",
            "We're patching routing logic and restoring assignments.",
            "Escalate to Engineering",
            "Need payload sample comparisons from release window.",
            78.0,
            82.0,
            76.0,
            80.0,
            79.0,
            "Good investigation depth; include clearer customer reassurance on timeline.",
            '["Schema-aware troubleshooting", "Useful action plan"]',
            '["Customer communication could be more explicit"]',
            "Tie rule conditions directly to nullable Urgency Tier paths.",
            "We've identified the routing mismatch and are applying a compatible rule update now.",
            "Schema contract tests prevent routing regressions.",
            "2026-04-03 12:05:00",
        ),
    ]
    executemany(
        """
        INSERT INTO attempts (
            user_id, module_id, organization_id, diagnosis_answer, next_steps_answer, customer_response,
            escalation_choice, notes, understanding_score, investigation_score, solution_score, communication_score,
            total_score, ai_feedback, strengths, missed_points, best_practice_reasoning,
            recommended_response, takeaway_summary, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        attempts,
    )


def backfill_existing_data() -> None:
    org_id = _ensure_org()
    execute("UPDATE users SET organization_id = COALESCE(organization_id, ?)", (org_id,))
    execute("UPDATE users SET is_active = COALESCE(is_active, 1)")
    execute("UPDATE users SET email = LOWER(REPLACE(name, ' ', '.')) || '@acmehealth.example' WHERE email IS NULL")
    execute("UPDATE modules SET organization_id = COALESCE(organization_id, ?)", (org_id,))
    execute("UPDATE modules SET status = COALESCE(status, 'published')")
    execute("UPDATE attempts SET organization_id = COALESCE(organization_id, ?)", (org_id,))


def sync_uuid_backed_learning_tables() -> None:
    # Keep user IDs stable and role-aligned so seeded/local accounts match admin UI expectations.
    # Existing non-empty IDs are preserved.
    execute(
        """
        UPDATE users
        SET id = 'u_' || COALESCE(role, 'learner') || '_' || CAST(user_id AS TEXT)
        WHERE id IS NULL OR TRIM(id) = ''
        """
    )

    # Ensure module external IDs exist for legacy integer keyed rows.
    module_id_expr = "LOWER(ENCODE(GEN_RANDOM_BYTES(16), 'hex'))" if RUNTIME_USE_POSTGRES else "LOWER(HEX(RANDOMBLOB(16)))"
    execute(f"UPDATE modules SET id = {module_id_expr} WHERE id IS NULL OR TRIM(id) = ''")

    # learner_profiles: derived from users + latest attempt activity.
    execute(
        f"""
        INSERT INTO learner_profiles (id, user_id, full_name, team, status, last_activity, created_at, updated_at)
        SELECT
            {module_id_expr} AS id,
            u.id AS user_id,
            COALESCE(u.name, u.email, 'Learner') AS full_name,
            u.team AS team,
            CASE WHEN COALESCE(u.is_active, 1) = TRUE THEN 'active' ELSE 'inactive' END AS status,
            MAX(a.created_at) AS last_activity,
            COALESCE(u.created_at, CURRENT_TIMESTAMP) AS created_at,
            CURRENT_TIMESTAMP AS updated_at
        FROM users u
        LEFT JOIN attempts a ON a.user_id = u.user_id
        WHERE u.role = 'learner' AND u.id IS NOT NULL
        GROUP BY u.user_id, u.id, u.name, u.email, u.team, u.is_active, u.created_at
        ON CONFLICT(user_id) DO UPDATE SET
            full_name = excluded.full_name,
            team = excluded.team,
            status = excluded.status,
            last_activity = COALESCE(excluded.last_activity, learner_profiles.last_activity),
            updated_at = CURRENT_TIMESTAMP
        """
    )

    # module_assignments: mirror legacy assignments using UUID-like user/module IDs.
    execute(
        f"""
        INSERT INTO module_assignments (id, user_id, module_id, assigned_at, assigned_by, created_at)
        SELECT
            {module_id_expr} AS id,
            lu.id AS user_id,
            lm.id AS module_id,
            asg.assigned_at,
            au.id AS assigned_by,
            COALESCE(asg.assigned_at, CURRENT_TIMESTAMP) AS created_at
        FROM assignments asg
        JOIN users lu ON lu.user_id = asg.learner_id
        JOIN modules lm ON lm.module_id = asg.module_id
        LEFT JOIN users au ON au.user_id = asg.assigned_by
        WHERE lu.id IS NOT NULL AND lm.id IS NOT NULL
        ON CONFLICT(user_id, module_id) DO UPDATE SET
            assigned_at = excluded.assigned_at,
            assigned_by = excluded.assigned_by
        """
    )

    # module_progress: 100% if completed attempt exists; otherwise 0%.
    execute(
        f"""
        INSERT INTO module_progress (
            id, user_id, module_id, progress_percent, started_at, completed_at, last_activity_at, created_at, updated_at
        )
        SELECT
            {module_id_expr} AS id,
            u.id AS user_id,
            m.id AS module_id,
            CASE WHEN MAX(att.created_at) IS NOT NULL THEN 100 ELSE 0 END AS progress_percent,
            MIN(asg.assigned_at) AS started_at,
            MAX(att.created_at) AS completed_at,
            MAX(att.created_at) AS last_activity_at,
            COALESCE(MIN(asg.assigned_at), CURRENT_TIMESTAMP) AS created_at,
            CURRENT_TIMESTAMP AS updated_at
        FROM assignments asg
        JOIN users u ON u.user_id = asg.learner_id
        JOIN modules m ON m.module_id = asg.module_id
        LEFT JOIN attempts att ON att.user_id = asg.learner_id AND att.module_id = asg.module_id
        WHERE u.id IS NOT NULL AND m.id IS NOT NULL
        GROUP BY u.id, m.id
        ON CONFLICT(user_id, module_id) DO UPDATE SET
            progress_percent = excluded.progress_percent,
            started_at = COALESCE(module_progress.started_at, excluded.started_at),
            completed_at = excluded.completed_at,
            last_activity_at = excluded.last_activity_at,
            updated_at = CURRENT_TIMESTAMP
        """
    )


def seed_all() -> None:
    _ensure_org()
    user_ids = seed_users()
    seed_modules()
    seed_assignments_and_attempts(user_ids)
    backfill_existing_data()
    sync_uuid_backed_learning_tables()
