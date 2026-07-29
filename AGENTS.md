# AGENTS.md

Repo-wide guidance for automated or assisted code changes in this project.

## Environment Assumptions

- Assume MariaDB storage in both production and developmemt.
- Assume direct access to replicas in production.
- In local development, replicas are available through an SSH tunnel on `localhost`.
- The default configuration should be for ToolForge.  Override using .env for local dev.

## Implementation Preferences

- Prefer batch-first solutions when they fit the task.
- Favor a single `UPSERT` statement over multiple row-by-row writes when possible.
- Keep changes small and aligned with the existing code style.
- Keep database access tight. Avoid reading before writing.
- Chunk writes into separate transactions where appropriate.
- Continuous tasks should look for work, execute, and sleep on a cycle of a few seconds.
- Dates should be stored as UTC. For display, ISO-8601 is preferred. A UI may show browser local time.
- All P and Q numbers given in code should be accompanied by their label in a comment.
- If I add deprecation warnings, don't remove them.

## Main cache

It is critical that the payload size of the main cache be kept as low as possible, using bitfields and constrained integer types. Explicit permission is required to increase the payload size.

## CLI

- All tasks are dispatched as options on `main.py`.
- A JSON manifest describes the supported ways of invoking `main.py`.
- Tasks can be launched singly using `--name` or in groups using `--group`.

## Workflow

- Inspect the existing code before changing behavior.
- Prefer non-destructive edits.
- Run the smallest useful verification step after changes when practical.
