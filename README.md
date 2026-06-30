# wd_notability

Scaffold for a Wikidata notability evaluation service.

Planned stages:
1. Notebook-based detector experimentation
2. Simple on-demand API server
3. Cached backend workers and queueing
4. Gadget integration

## Quick start

```bash
uv sync
uv run uvicorn server.app:app --reload
```

The web process does not run background workers by default. Run workers in a
separate shell when you want to process the queue:

```bash
python main.py worker --workers 3 --poll-seconds 1
```

## Example API

```bash
curl http://127.0.0.1:8000/api/evaluate/Q42
```

## Build Offline Caches

This project now expects a lookup cache database for namespace lookups,
API URLs, and property-instance sets.

Generate namespace cache and API URL list:

```bash
uv run python main.py build-namespace-cache
```

Local/dev output:

- `~/localdbs/wd-notability/lookup_cache.db`

Generate SPARQL property-instance cache:

```bash
uv run python main.py build-property-cache
```

Local/dev output:

- `~/localdbs/wd-notability/lookup_cache.db`

Runtime note:

- Namespace and property-instance lookups are loaded from
  `~/localdbs/wd-notability/lookup_cache.db`.

Toolforge production note:

- Toolforge auto-detects `~/replica.my.cnf`, uses the Toolforge ToolsDB
  backend, and derives the database name from the credential username.
- Toolforge does not use the local `~/localdbs/...` cache files.
- The ToolDB name follows the ToolsDB naming convention in the Toolforge docs
  and is derived from the credential username in `replica.my.cnf`.
- Optionally set `WD_NOTABILITY_LOOKUP_HOST` to
  `tools.db.svc.wikimedia.cloud` or `tools-readonly.db.svc.wikimedia.cloud`.
- See [docs/toolforge-playbook.md](docs/toolforge-playbook.md) for the build,
  webservice, and job-deployment workflow.

If you only want a quick dry run, add `--limit 20` to
`main.py build-namespace-cache`.

Environment template:

- Copy [`.env.example`](/Users/grm/Documents/GitHub/wd-notability/.env.example) to a local `.env` if you want a starter set of variables.
- The package loads `.env` automatically on import if a file exists in the current working directory or project root.
