---
name: Offline telemetry storage
description: The telemetry product's storage constraint for event logs and quarantine state.
---

Keep telemetry event history and quarantine state in local SQLite, with no hosted database dependency. Preserve the browser's local pending-event queue and cached snapshot so the dashboard can operate during API outages.

**Why:** The user explicitly requested an offline-first system with local SQLite persistence and no external cloud dependencies.

**How to apply:** When changing ingestion, state persistence, or offline behavior, preserve this local-first design unless the user changes the requirement.
