# opensourcelaw pipelines

This repository uses a raw Nix flake for Python and Dagster development.

The flake keeps the setup small and portable for Linux and macOS without requiring an additional tool such as `devenv`. Nix provides Python 3.12 and `uv`; `uv.lock` is converted into a Nix-built virtualenv with `uv2nix`.

## Environment

Use the Nix dev shell as the only Python environment. It builds the Python 3.12 virtualenv from `uv.lock`; do not run `uv sync` or create a local `.venv` for this repo.

Allow direnv once:

```sh
direnv allow
```

Then open Zed from the launcher. Do not launch Zed from inside `nix develop` or an already-loaded direnv shell.

Zed is configured to start Basedpyright and Roslyn through `nix develop`, so Python analysis and C# analysis see the Nix-provided Python virtualenv and .NET SDK even when Zed was opened from the launcher. Stale `.venv` or `result` entries are disposable local artifacts.

For agents and shells without direnv, prefix commands with `nix develop -c`, for example `nix develop -c python -m pytest`.

## Usage

Enter the development shell:

```sh
nix develop
```

Run Python with Dagster available:

```sh
python -c "import dagster; print(dagster.__version__)"
```

Validate the Dagster definitions:

```sh
dagster definitions validate
```

Start the local Dagster UI:

```sh
dagster dev
```

## Local S3 Viewer

Start the SeaweedFS S3 endpoint:

```sh
sudo docker compose -f compose.dev.yaml up -d seaweedfs
```

Open it in STU:

```sh
stu-seaweedfs
```

To jump straight to the pipeline's default raw bucket:

```sh
stu-seaweedfs --bucket opensourcelaw-raw
```

## Retsinformation Raw Ingest

The first Dagster asset chain fetches raw source data from `retsinformation.dk`, then parses downloaded XML:

```text
retsinfo_sitemap_index -> retsinfo_sitemap_pages -> retsinfo_documents -> retsinfo_parsed_documents
```

This intentionally stops before normalization, embedding, or vector index syncing.

Durable local outputs are written under `data/ingest` by default:

```text
data/ingest/discovery/sitemap_pages/...
data/ingest/raw/retsinformation_eli/...
data/ingest/raw/retsinformation_documents/<document_type>/<year>/xml/*.xml
data/ingest/raw/retsinformation_documents/<document_type>/<year>/failures.jsonl
data/ingest/raw/retsinformation_documents/<document_type>/<year>/manifest.json
data/ingest/parsed/retsinformation_documents/<document_type>/<year>/*.jsonl
data/ingest/parsed/retsinformation_documents/<document_type>/<year>/manifest.json
data/ingest/metadata/raw_fetches.jsonl
data/ingest/metadata/changed_raw_fetches.jsonl
data/ingest/runs/<dagster-run-id>/...
```

Document downloads are materialized as metadata only: Dagster stores counts and file paths, while document bodies stay as XML files. Keep that boundary until an object store or real IO manager is needed.

Useful local overrides:

```sh
export OPENSOURCELAW_INGEST_ROOT=/tmp/opensourcelaw-ingest
export OPENSOURCELAW_RETSINFO_SITEMAP_PAGES=1
export OPENSOURCELAW_RETSINFO_MAX_ITEMS=25
```

To use an explicit source config, start from:

```sh
export OPENSOURCELAW_SOURCES_FILE=config/retsinformation.sources.example.json
```

The built-in default is intentionally capped to one sitemap page and 25 raw fetches so a developer does not accidentally mirror the whole site. For a full run, use the example config with `sitemap_pages` set to `19` and `max_items` set to `null`.

The storage boundary is currently filesystem-based. A local RustFS/S3 setup would make sense once an S3 object-store implementation is added behind the same store interface; for this first milestone, the filesystem store keeps local runs simple and testable.

## Dependencies

Add Python dependencies with `uv`, then re-enter the Nix shell so `uv2nix` can rebuild the virtualenv from the updated lockfile:

```sh
uv add --no-sync dagster-postgres
nix develop
```
