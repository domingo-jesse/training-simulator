from db import execute, executemany, fetch_one


def seed_users() -> None:
    if fetch_one("SELECT user_id FROM users LIMIT 1"):
        return
    users = [
        ("Ava Patel", "learner", "Operations"),
        ("Jordan Lee", "learner", "Revenue Cycle"),
        ("Sam Rivera", "learner", "Platform Support"),
        ("Mia Chen", "learner", "Clinical Ops"),
        ("Admin User", "admin", "Training"),
    ]
    executemany("INSERT INTO users (name, role, team) VALUES (?, ?, ?)", users)


def seed_modules() -> None:
    if fetch_one("SELECT module_id FROM modules LIMIT 1"):
        return

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
        ),
    ]

    for module in modules:
        module_id = execute(
            """
            INSERT INTO modules (
                title, category, difficulty, description, estimated_time,
                scenario_ticket, scenario_context, hidden_root_cause,
                expected_reasoning_path, expected_diagnosis, expected_next_steps,
                expected_customer_response, lesson_takeaway
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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


def seed_all() -> None:
    seed_users()
    seed_modules()
