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

## 2. Run the webserver

Start the webservice from the image you built:

```bash
toolforge webservice buildservice start --mount=all
```

Useful follow-up commands:

```bash
toolforge webservice buildservice logs -f
toolforge webservice restart
```

Note:

- `--mount=all` keeps `~/replica.my.cnf` available for the DB credentials this repository expects.
- This repository currently reads database credentials from `~/replica.my.cnf` in several places.
- The username in that file is what Toolforge uses to derive the tool database name.
- There is no separate production setting for the ToolDB name in this playbook.
- The app auto-detects Toolforge from the mounted credential file, so no
  Toolforge-specific database env vars are needed here.
- Toolforge supplies the web `PORT` for the container, so the Toolforge command does not hardcode a port here.
- Toolforge jobs use the MariaDB-backed cache stores, so no local cache database files are created on Toolforge.
- Any `~/localdbs/...` paths in this repository are for local/dev runs only.
- Keep `--mount=all` so `~/replica.my.cnf` stays available; the mounted Toolforge home directory is what keeps the runtime credentials and other tool-owned state available across runs.

## 3. Deploy jobs

`main.py` maps the runtime manifest to Toolforge jobs. The `toolforge` group now includes every deployable unit.

### Bootstrap jobs

Use this when you want the initial cache-building pass as one-shot runs:

```bash
python3 main.py deploy --group bootstrap --once --mount all
```

### Continuous jobs

Use this for long-running workers:

```bash
python3 main.py deploy --group continuous --mount all
```

### Scheduled jobs

Use this for recurring cache builders:

```bash
python3 main.py deploy --group scheduled --mount all
```

### Full deploy

If you want to deploy the whole Toolforge footprint in one shot:

```bash
python3 main.py deploy --mount all
```

## 4. Stop jobs

When you need to remove jobs from Toolforge:

```bash
python3 main.py stop --group continuous
python3 main.py stop --group scheduled
python3 main.py stop --group bootstrap
```

## 5. Unit mapping

- `webserver` is the Toolforge web process.
- `continuous` contains the always-on workers.
- `scheduled` contains the recurring cache builders.
- `bootstrap` is the same cache-builder set, used for the initial pass.

## 6. Recommended workflow

1. Build the image.
2. Start the webservice.
3. Deploy `bootstrap` jobs once with `--once --mount all`.
4. Deploy `scheduled` jobs for the ongoing cadence.
5. Deploy `continuous` workers for the background loops.

The runtime depends on `~/replica.my.cnf`, so mounted storage is part of the normal Toolforge setup here.
