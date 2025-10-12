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
- Docker support is expected for local development and CI.

## TODO
The following feature backlog should be tackled in order across future sessions:

1. Compute utilization and availability metrics for ports, stations, and districts (session-count utilization, charger-occupation utilization, active-charging utilization, availability ratio).
2. Distribution and temporal pattern analysis, including richer duration statistics, occupancy heatmaps, and longer-term trend exploration.
3. Reliability and downtime analysis with MTBF/MTTR calculations, outage tracking, and visualisations.
4. Geospatial and cluster analysis that ties utilization and availability metrics to location data and external datasets.
5. Predictive and anomaly detection models for demand forecasting and adaptive thresholding.

## Completed
- Dockerise the loop functionality so it can run unattended.
- Create a script to push the updated site via git on a schedule.
