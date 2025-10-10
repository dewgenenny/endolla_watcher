# Endolla Watcher

Endolla Watcher analyses the Barcelona Endolla open data feed to highlight
charging ports that appear unused, experience only short sessions or are marked
unavailable for extended periods. The project now ships as two deployable
components: a Python backend that ingests data and exposes a JSON API, and a
lightweight frontend that renders the dashboard in the browser.

## Development

Create a virtual environment and install the project in editable mode to work on
the backend:

```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

The backend authenticates with MySQL using the `caching_sha2_password` plugin by
default, which requires the `cryptography` package. Installing dependencies via
`requirements.txt` or the project metadata pulls it in automatically.

Start the FastAPI backend with uvicorn. Provide a MySQL URL and disable the
fetch loop so you can feed fixture data manually during development:

```
ENDOLLA_DB_URL=mysql+pymysql://user:pass@localhost:3306/endolla \
ENDOLLA_AUTO_FETCH=0 uvicorn endolla_watcher.api:app --reload
```

You can still generate static reports for debugging with the legacy CLI tools:

```
python -m endolla_watcher.main --file endolla.json --output site/index.html
python -m endolla_watcher.loop --fetch-interval 60 --update-interval 3600 \
    --db-url mysql+pymysql://user:pass@localhost:3306/endolla \
    --unused-days 7 --long-session-days 2 \
    --long-session-min 5 --unavailable-hours 24
```

The frontend lives under `frontend/` and is plain HTML/CSS/JS. Serve it with any
static web server during development:

```
python -m http.server --directory frontend 8080
```

Set `window.ENDOLLA_API_BASE` in your browser console if you need to point the
frontend at a remote backend while developing locally.

## Database

Snapshots are now stored in MySQL. Old records are automatically pruned so the
database only keeps the last four weeks of history. The application checks the
schema version when it opens the database and applies any pending migrations
automatically.

If you are migrating from a previous SQLite deployment run the migration tool
once to import existing history:

```
python -m endolla_watcher.migrate --sqlite endolla.db \
    --db-url mysql+pymysql://user:pass@localhost:3306/endolla
```

To review the current size and reclaim unused space you can run:

```
python -m endolla_watcher.db --db-url mysql+pymysql://user:pass@localhost:3306/endolla --compress
```

Omit `--compress` to only display basic database statistics.

## Containers

Two containers are published from this repository:

* **Backend:** `docker build -t endolla-watcher-backend .` produces the FastAPI
  service. Environment variables listed in `deploy/backend-configmap.yaml`
  control fetch cadence and rule thresholds.
* **Frontend:** `docker build -t endolla-watcher-frontend frontend/` builds a
  static NGINX image that serves the dashboard assets.

Run the backend locally by pointing it at a MySQL instance reachable from the
container:

```
docker run -p 8000:8000 \
           -e ENDOLLA_DB_URL=mysql+pymysql://user:pass@host:3306/endolla \
           endolla-watcher-backend
```

Serve the frontend alongside it with:

```
docker run -p 8080:80 endolla-watcher-frontend
```

Configure your reverse proxy so that `/api` traffic is forwarded to the backend
while other paths are served by the frontend container.

## Argo CD deployment

Kubernetes manifests suitable for Argo CD live under the `deploy/` directory.
They provision separate deployments for the backend API and the static frontend,
alongside an in-cluster MySQL StatefulSet that keeps data on a persistent
volume. Credentials for the database are defined in
`deploy/mysql-secret.yaml`, and the backend ConfigMap points to the
cluster-internal service name. The manifests are managed via Kustomize and can
be synchronised by Argo CD using the example application specification in
`argocd/application.yaml`.

Update the ConfigMap with any custom rule values, adjust the container image
references and rotate the database credentials as required. Configure your
ingress controller to route `/api` to the backend service. Everything else can
point to the frontend service to deliver the updated dashboard without relying
on GitHub Pages.
