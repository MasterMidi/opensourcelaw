# opensourcelaw pipelines

This repository uses a raw Nix flake for Python and Dagster development.

The flake keeps the setup small and portable for Linux and macOS without requiring an additional tool such as `devenv`. Nix provides Python 3.12 and `uv`; `uv.lock` is converted into a Nix-built virtualenv with `uv2nix`.

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

## Dependencies

Add Python dependencies with `uv`, then re-enter the Nix shell so `uv2nix` can rebuild the virtualenv from the updated lockfile:

```sh
uv add --no-sync dagster-postgres
nix develop
```
