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

Start the FastAPI backend with uvicorn. The example below stores the SQLite
database in the repository directory and disables the fetch loop so you can feed
fixture data manually during development:

```
ENDOLLA_DB_PATH=./endolla.db ENDOLLA_AUTO_FETCH=0 uvicorn endolla_watcher.api:app --reload
```

You can still generate static reports for debugging with the legacy CLI tools:

```
python -m endolla_watcher.main --file endolla.json --output site/index.html
python -m endolla_watcher.loop --fetch-interval 60 --update-interval 3600 \
    --db endolla.db --unused-days 7 --long-session-days 2 \
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

Snapshots are stored in a SQLite database. Old records are automatically pruned
so the database only keeps the last four weeks of history. The application
checks the schema version when it opens the database and applies any pending
migrations automatically. If you prefer to upgrade a database manually you can
run:

```
python -m endolla_watcher.migrate --db endolla.db
```

To review the current size and reclaim unused space you can run:

```
python -m endolla_watcher.db --db endolla.db --compress
```

Omit `--compress` to only display basic database statistics.

## Containers

Two containers are published from this repository:

* **Backend:** `docker build -t endolla-watcher-backend .` produces the FastAPI
  service. Environment variables listed in `deploy/backend-configmap.yaml`
  control fetch cadence and rule thresholds.
* **Frontend:** `docker build -t endolla-watcher-frontend frontend/` builds a
  static NGINX image that serves the dashboard assets.

Run the backend locally with persistent storage mounted at `/data` to keep the
SQLite database between restarts:

```
docker run -p 8000:8000 \
           -e ENDOLLA_DB_PATH=/data/endolla.db \
           -v $(pwd)/data:/data \
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
They provision separate deployments for the backend API and the static frontend.
The backend is backed by a persistent volume claim so the SQLite database
survives pod restarts. The manifests are managed via Kustomize and can be
synchronised by Argo CD using the example application specification in
`argocd/application.yaml`.

Update the ConfigMap with any custom rule values, adjust the container image
references and configure your ingress controller to route `/api` to the backend
service. Everything else can point to the frontend service to deliver the
updated dashboard without relying on GitHub Pages.
