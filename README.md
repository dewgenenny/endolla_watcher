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
    --db endolla.db --unused-days 4 --long-session-days 2 \
    --long-session-min 5 --unavailable-hours 24
```

The site can then be served from the `site/` directory. It now features a small
Bootstrap-based theme and an `about.html` page with project details.

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
image and pushes it to Docker Hub. Configure the `DOCKERHUB_USERNAME` and
`DOCKERHUB_TOKEN` secrets for authentication.

Run the Docker container on your own server and generate the site locally.
Set the `GH_TOKEN` environment variable to a GitHub token with permission to
push to the repository and provide the repository URL via `REPO_URL`. When the
container is started with the `--push-site` flag, the site will be committed and
pushed to the `gh-pages` branch automatically after each update interval.
The commit author can be configured with `GH_NAME` and `GH_EMAIL`.
