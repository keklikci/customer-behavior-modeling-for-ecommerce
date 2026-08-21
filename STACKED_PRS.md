# Stacked change plan

The repository is organized as these independently reviewable layers:

1. `codex/tooling` adds uv metadata, Ruff configuration, pytest configuration,
   and the Python ignore rules
2. `codex/tests` adds dependency-light feature math helpers and unit tests
3. `codex/cli` makes Spark scripts accept explicit paths and stop sessions
4. `codex/docs` documents local development and Spark usage

Each layer should be based on the previous layer and merged in order. The
current workspace cannot write Git refs, so these names are a review plan only;
the files are present together in the working tree.

## Validation

```bash
uv sync
uv run ruff format --check .
uv run ruff check .
uv run pytest
```

