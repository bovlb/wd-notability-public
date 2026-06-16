# wd_notability

Scaffold for a Wikidata notability evaluation service.

Planned stages:
1. Notebook-based detector experimentation
2. Simple on-demand API server
3. Cached backend workers and queueing
4. Gadget integration

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn server.app:app --reload
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
./.venv/bin/python scripts/build_namespace_cache.py
```

Outputs:

- `wd_notability/data/lookup_cache.db`

Generate SPARQL property-instance cache:

```bash
./.venv/bin/python scripts/build_property_cache.py
```

Output:

- `wd_notability/data/lookup_cache.db`

Runtime note:

- Namespace and property-instance lookups are loaded from
  `wd_notability/data/lookup_cache.db`.

Toolforge production note:

- Set `WD_NOTABILITY_LOOKUP_BACKEND=mariadb` to use the Toolforge ToolsDB
  backend.
- Set `WD_NOTABILITY_LOOKUP_DATABASE` to your Toolforge database name, which
  should follow the ToolsDB naming convention described in the Toolforge docs
  (`<credentialUser>__<DBName>`).
- Optionally set `WD_NOTABILITY_LOOKUP_HOST` to
  `tools.db.svc.wikimedia.cloud` or `tools-readonly.db.svc.wikimedia.cloud`.

If you only want a quick dry run, add `--limit 20` to
`scripts/build_namespace_cache.py`.

Environment template:

- Copy [`.env.example`](/Users/grm/Documents/GitHub/wd-notability/.env.example) to a local `.env` if you want a starter set of variables.
- The package loads `.env` automatically on import if a file exists in the current working directory or project root.
