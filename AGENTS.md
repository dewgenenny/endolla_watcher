# endolla-watcher Agent

This project monitors the Barcelona Endolla open data to find underused or out-of-service chargers.

## Ideas
- Fetch data regularly from the Endolla open data API (JSON endpoint).
- Track usage sessions or, if unavailable, rely on `last_updated` timestamps.
- Identify chargers with no sessions or sessions shorter than three minutes.
- Generate a simple HTML page summarising problematic chargers.
- Automate updates by running Docker locally and using `push_site.py` to push
  to `gh-pages`.

## Notes for future agents
- Prefer Python for scraping and analysis.
- Keep dependencies minimal and list them in `requirements.txt`.
- Docker support is expected for local development.

## TODO
_None_

## Completed
- Dockerise the loop functionality so it can run unattended.
- Create a script to push the updated site via git on a schedule.
