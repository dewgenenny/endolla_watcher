# endolla-watcher Agent

This project monitors the Barcelona Endolla open data to find underused or out-of-service chargers.

## Ideas
- Fetch data regularly from the Endolla open data API (JSON endpoint).
- Track usage sessions or, if unavailable, rely on `last_updated` timestamps.
- Identify chargers with no sessions or sessions shorter than three minutes.
- Generate a simple HTML page summarising problematic chargers.
- Automate updates via GitHub Actions pushing to `gh-pages`.

## Notes for future agents
- Prefer Python for scraping and analysis.
- Keep dependencies minimal and list them in `requirements.txt`.
- Docker support is expected for local development and CI.
