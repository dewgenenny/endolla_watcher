# Endolla Watcher

This project analyses the Barcelona Endolla open data feed to highlight charging
ports that appear unused or have very short sessions. The results are published
as a GitHub Pages site.

## Development

```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e .
python -m endolla_watcher.main --file endolla.json --output site/index.html
python -m endolla_watcher.loop --fetch-interval 60 --update-interval 3600 \
    --db endolla.db --unused-days 7 --long-session-days 2 \
    --long-session-min 5 --unavailable-hours 24
```

The site can then be served from the `site/` directory. It now features a small
Bootstrap-based theme, a weekly history graph, the average charging time over
the last 24 hours, the number of short charging sessions and an `about.html`
page with project details. Problematic chargers link to individual pages
showing their most recent charging sessions.

## Database

Snapshots are stored in a SQLite database. Old records are automatically
pruned so the database only keeps the last four weeks of history. The
application checks the schema version when it opens the database and applies
any pending migrations automatically. If you prefer to upgrade a database
manually you can run:

```
python -m endolla_watcher.migrate --db endolla.db
```

To review the current size and reclaim unused space you can run:

```
python -m endolla_watcher.db --db endolla.db --compress
```

Omit `--compress` to only display basic database statistics.

## Docker

```
docker build -t endolla-watcher .
docker run -v $(pwd)/endolla.db:/data/endolla.db \
           -v $(pwd)/site:/data/site \
           -e GH_TOKEN=YOURTOKEN \
           -e REPO_URL=https://github.com/you/repo.git \
           -e GH_NAME="Your Name" \
           -e GH_EMAIL=you@example.com \
           endolla-watcher \
           --fetch-interval 300 --update-interval 3600 \
           --push-site
```

The entrypoint fetches the dataset from the public API and sets the `--output`
and `--db` paths under `/data`. Any arguments provided when running the image
are appended to those defaults, so you only need to specify the options you wish
to change, such as the fetch interval or `--push-site`. To analyse a specific
dataset file instead of the live data, start the container with
`--file /path/to/file`.

The image contains `git` and the `push_site.py` helper so updates can be
published directly from within the container. Provide the GitHub token and
repository URL as shown above to enable automatic pushes.

## Automation

A GitHub Actions workflow (`.github/workflows/docker.yml`) builds the Docker
image and publishes it to the GitHub Container Registry (GHCR) under
`ghcr.io/<repository-owner>/endolla-watcher`. Authentication is handled via the
repository's default `GITHUB_TOKEN`, so no additional secrets are required
provided the workflow has `packages: write` permission. Consumers can pull the
image with:

```
docker pull ghcr.io/<repository-owner>/endolla-watcher:latest
```

Run the Docker container on your own server and generate the site locally.
Set the `GH_TOKEN` environment variable to a GitHub token with permission to
push to the repository and provide the repository URL via `REPO_URL`. When the
container is started with the `--push-site` flag, the site will be committed and
pushed to the `gh-pages` branch automatically after each update interval.
The commit author can be configured with `GH_NAME` and `GH_EMAIL`.

## Argo CD deployment

Kubernetes manifests suitable for Argo CD live under the `deploy/` directory.
They package the watcher container as a single-replica deployment backed by a
persistent volume claim so the SQLite database and rendered site survive pod
restarts. The manifests are managed via Kustomize and can be synchronised by
Argo CD using the example application specification in `argocd/application.yaml`.
Update the ConfigMap with your Git repository URL and commit author details,
add a GitHub token to the accompanying secret, then apply the Argo CD
application to roll out the watcher in your cluster.
