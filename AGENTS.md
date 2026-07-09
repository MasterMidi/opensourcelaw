# AGENTS.md

Use `jj` (Jujutsu) for version control in this repository.
Always write a `jj` change message for changes you make, for example with `jj describe -m "..."`.
Always use the ponytail skill for development
Use the Nix dev shell as the Python environment. Do not create `.venv`; run Python commands with `nix develop -c ...` unless direnv has already loaded `.envrc`.
Prefer offloading non-Dagster logic and heavy lifting to scripts in other languages, especially C# when reasonable. Keep Dagster orchestration in Python; use Python/Dagster directly when offloading would be more work than it saves.
For Dagster work, prefer the `dg` CLI where it fits, such as `uv run dg ...` or `nix develop -c dg ...`; fall back to Python or legacy `dagster` commands when `dg` does not cover the task.
