"""oservi.api_gateway — Agent OS route registry (host-agnostic).

3O layer: oservi (engine assembly).
Declarative route table for the host gateway (FastAPI / other):
the host imports ROUTE_TABLE and mounts each route to its web framework.
No web framework dependency in the main library.
"""

from __future__ import annotations

# (method, path, handler_name, description)
ROUTE_TABLE: list[tuple[str, str, str, str]] = [
    ("POST", "/master/chat", "master_chat", "Master brain chat: intent routing + tool calls"),
    ("GET", "/master/tools", "master_tools", "List master-visible tool schemas"),
    ("GET", "/automata/jobs", "automata_jobs", "List background automation jobs"),
    ("POST", "/automata/jobs", "automata_create_job", "Register a Cron automation job"),
    ("DELETE", "/automata/jobs/{task_id}", "automata_remove_job", "Cancel an automation job"),
    ("POST", "/automata/run-now", "automata_run_now", "Trigger one headless run immediately"),
    ("POST", "/webhooks/{source}", "webhook_event", "External event entry (Github/CI/webhook)"),
    ("POST", "/api/v1/tasks/{task_id}/approve", "vault_approve", "Human approval for vault HITL"),
    ("GET", "/vault/secrets", "vault_secrets", "List vault secret IDs (never plaintext)"),
    ("GET", "/vault/pending", "vault_pending", "List pending HITL approvals"),
]

ROUTE_DESCRIPTION = "Veya Agent OS gateway routes (host mounts these onto its web framework)"
