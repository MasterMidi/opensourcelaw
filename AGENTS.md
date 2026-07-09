# AGENTS.md

Use `jj` (Jujutsu) for version control in this repository.
Always write a `jj` change message for changes you make, for example with `jj describe -m "..."`.
Always use the ponytail skill for development
Prefer offloading non-Dagster logic and heavy lifting to scripts in other languages, especially C# when reasonable. Keep Dagster orchestration in Python; use Python/Dagster directly when offloading would be more work than it saves.
