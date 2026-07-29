# Toolforge Playbook

This playbook matches the current runtime manifest in `wd_notability/data/runtime_units.json` and the Toolforge build service / jobs framework.

## 1. Build the image

Toolforge builds the app from a public Git repository using the build service.

```bash
become wd-notability
toolforge build start https://github.com/bovlb/wd-notability-public
```

Notes:

- The repo needs a `Procfile` at the root.
- This project already has `Procfile` set to run `/workspace/scripts/toolforge-entrypoint.sh`, which exports `LD_LIBRARY_PATH` and then launches the app with the buildpack venv Python binary.
- Toolforge's build service should install the Python dependencies from the checked-in `uv.lock`.
- If you want to test the image locally first, follow the current Toolforge build-service docs for a local image build workflow.
- The built Toolforge image is the Python environment on Toolforge, so there is no separate virtualenv activation step in the deploy commands.
- The buildpack image exposes the runtime environment under `/layers/heroku_python/venv/bin/python3`; the wrapper at `/workspace/scripts/toolforge-entrypoint.sh` sets `LD_LIBRARY_PATH=/layers/heroku_python/python/lib` so the shared library loader can find `libpython3.13.so.1.0`.
- Local `uv` usage stays local; Toolforge uses the image you built from the repo.

## 2. Set env vars

Use a local `.env` file as the source of truth for configuration, then push the
deploy-time values into Toolforge envvars.

If you want to load a `.env` file into your shell explicitly:

```bash
set -a
source .env
set +a
```

Required env vars:

- `REPLICADB_USER`, `REPLICADB_PASSWORD`, `REPLICADB_HOST`, `REPLICADB_PORT`,
  `REPLICADB_DATABASE`
- `TOOLSDB_HOST`, `TOOLSDB_PORT`, `TOOLSDB_DATABASE`, `TOOLSDB_USER`,
  `TOOLSDB_PASSWORD`

Create the ToolsDB database once if it does not already exist:

```bash
sql tools
CREATE DATABASE s57749__wd_notability;
```

After that, create or update the Toolforge envvars for the same values and
restart the webservice/jobs so they pick up the changes.

## 3. Run the webserver

Start the webservice from the image you built:

```bash
toolforge webservice buildservice start --mount=none
```

Useful follow-up commands:

```bash
toolforge webservice buildservice logs -f
toolforge webservice restart
```

Notes:

- Toolforge supplies the web `PORT` for the container, so the Toolforge command does not hardcode a port here.
- Toolforge jobs use the MariaDB-backed cache stores, so no local cache database files are created on Toolforge.
- Any cache-root paths in this repository are for local/dev runs only.
- We use `--mount=none` so the runtime depends on env vars instead of home-directory state.

## 4. Deploy jobs

`main.py` maps the runtime manifest to Toolforge jobs. The `toolforge` group now includes every deployable unit.

### Bootstrap jobs

Use this when you want the initial cache-building pass as one-shot runs:

```bash
python3 main.py deploy --group bootstrap --once --mount none
```

### Continuous jobs

Use this for long-running workers:

```bash
python3 main.py deploy --group continuous --mount none
```

### Scheduled jobs

Use this for recurring cache builders:

```bash
python3 main.py deploy --group scheduled --mount none
```

### Full deploy

If you want to deploy the whole Toolforge footprint in one shot:

```bash
python3 main.py deploy --mount none
```

## 5. Stop jobs

When you need to remove jobs from Toolforge:

```bash
python3 main.py stop --group continuous
python3 main.py stop --group scheduled
python3 main.py stop --group bootstrap
```

## 6. Unit mapping

- `webserver` is the Toolforge web process.
- `continuous` contains the always-on workers.
- `scheduled` contains the recurring cache builders.
- `bootstrap` is the same cache-builder set, used for the initial pass.

## 7. Recommended workflow

1. Build the image.
2. Load `.env` into your shell.
3. Start the webservice.
4. Deploy `bootstrap` jobs once with `--once --mount none`.
5. Deploy `scheduled` jobs for the ongoing cadence.
6. Deploy `continuous` workers for the background loops.
