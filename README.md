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

The site can then be served from the `site/` directory.

## Docker

```
docker build -t endolla-watcher .
docker run -v $(pwd)/endolla.json:/data/endolla.json \
           -v $(pwd)/endolla.db:/data/endolla.db \
           -v $(pwd)/site:/data/site \
           endolla-watcher \
           --fetch-interval 300 --update-interval 3600
```

The image contains `git` and the `push_site.py` helper so updates can be
published directly from within the container.

## Automation

Run the Docker container on your own server and generate the site locally. The
`scripts/push_site.py` helper commits the contents of the `site/` directory to
the `gh-pages` branch and pushes the update. Schedule the container and helper
via cron or another task runner.
