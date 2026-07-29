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

For a local MariaDB cache store, start the container with:

```bash
scripts/run-local-mariadb.sh
```

Then populate the runtime env vars with:

```bash
TOOLSDB_HOST=localhost TOOLSDB_PORT=3306 eval "$(scripts/export-db-env.sh)"
```

The web process does not run background workers by default. Run workers in a
separate shell when you want to process the queue:

```bash
python main.py worker --workers 3 --poll-seconds 1
```

## Example API

```bash
curl http://127.0.0.1:8000/api/evaluate/Q4657574
```

## Build Offline Caches

This project now expects a lookup cache database for namespace lookups,
API URLs, and property-instance sets.

Generate namespace cache and API URL list:

```bash
uv run python main.py build-namespace-cache
```

Local/dev output:

- the cache root used by your environment

Generate SPARQL property-instance cache:

```bash
uv run python main.py build-property-cache
```

Local/dev output:

- the cache root used by your environment

Runtime note:

- Namespace and property-instance lookups are loaded from
  the cache root used by your environment.

Toolforge and local MariaDB note:

- `REPLICADB_USER`, `REPLICADB_PASSWORD`, `REPLICADB_HOST`,
  `REPLICADB_PORT`, `REPLICADB_DATABASE`, `TOOLSDB_HOST`, `TOOLSDB_PORT`,
  `TOOLSDB_DATABASE`, `TOOLSDB_USER`, and `TOOLSDB_PASSWORD` are required at
  runtime.
- You can populate those from a local `.env` file or from `replica.my.cnf`
  with `scripts/export-db-env.sh`.
- Toolforge does not create local cache database files.
- See [docs/toolforge-playbook.md](docs/toolforge-playbook.md) for the build,
  webservice, and job-deployment workflow.

If you only want a quick dry run, add `--limit 20` to
`main.py build-namespace-cache`.

Environment template:

- Copy [`.env.example`](/Users/grm/Documents/GitHub/wd-notability/.env.example) to a local `.env` if you want a starter set of variables.
- The package loads `.env` automatically on import if a file exists in the current working directory or project root.
