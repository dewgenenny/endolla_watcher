# Endolla Watcher

This project analyses the Barcelona Endolla open data feed to highlight charging
ports that appear unused or have very short sessions. The results are published
as a GitHub Pages site.

## Development

```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m endolla_watcher.main --file endolla.json --output site/index.html
python -m endolla_watcher.loop --interval 300
```

The site can then be served from the `site/` directory.

## Docker

```
docker build -t endolla-watcher .
docker run -v $(pwd)/endolla.json:/data/endolla.json endolla-watcher
```

## Automation

GitHub Actions (`.github/workflows/update.yml`) periodically runs the watcher
and deploys the generated HTML to the `gh-pages` branch.
