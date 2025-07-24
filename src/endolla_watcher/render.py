from typing import List, Dict, Any
import json
import logging

logger = logging.getLogger(__name__)


# Navigation bar shared across pages
NAVBAR = """
<nav class="navbar navbar-expand-lg navbar-dark bg-primary">
  <div class="container-fluid">
    <a class="navbar-brand" href="index.html">Endolla Watcher</a>
    <div class="collapse navbar-collapse">
      <ul class="navbar-nav ms-auto">
        <li class="nav-item"><a class="nav-link" href="about.html">About</a></li>
      </ul>
    </div>
  </div>
</nav>
"""

# Template for the main index page
INDEX_TEMPLATE = """
<!DOCTYPE html>
<html lang='en'>
<head>
    <meta charset='UTF-8'>
    <title>Endolla Watcher</title>
    <link href="https://cdn.jsdelivr.net/npm/bootswatch@5.3.2/dist/flatly/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns"></script>
</head>
<body>
{navbar}
<div class="container py-4">
<h1 class="mb-4">Network Overview</h1>
<ul class="list-group mb-4">
    <li class="list-group-item">Total chargers: {chargers}</li>
    <li class="list-group-item">Unavailable chargers: {unavailable}</li>
    <li class="list-group-item">Currently charging: {charging}</li>
    <li class="list-group-item">Total charging events: {sessions}</li>
    <li class="list-group-item">Short sessions (<5 min): {short_sessions}</li>
    <li class="list-group-item">Avg session (24h): {avg_session_min:.1f} min</li>
</ul>
<div class="mb-4">
    <canvas id="historyChart" height="80"></canvas>
</div>
<script>
{history_js}
</script>
<h2 class="mt-4">Problematic Chargers</h2>
<div class="table-responsive">
<table class="table table-striped">
    <thead class="table-dark">
        <tr><th>Location</th><th>Station</th><th>Port</th><th>Status</th><th>Reason</th></tr>
    </thead>
    <tbody>
        {rows}
    </tbody>
</table>
</div>
<div class="text-muted small mt-4">
    <p>Page last updated: {updated}</p>
    <p>DB size: {db_size:.1f} MB</p>
    <p>Processed in {elapsed:.2f} s</p>
</div>
</div>
</body>
</html>
"""

# Template for a charger details page
CHARGER_TEMPLATE = """
<!DOCTYPE html>
<html lang='en'>
<head>
    <meta charset='UTF-8'>
    <title>Charger {station_id}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootswatch@5.3.2/dist/flatly/bootstrap.min.css" rel="stylesheet">
</head>
<body>
{navbar}
<div class="container py-4">
<h1 class="mb-4">Charger {station_id}</h1>
<table class="table table-striped">
    <thead class="table-dark">
        <tr><th>Port</th><th>Start</th><th>End</th><th>Duration (min)</th></tr>
    </thead>
    <tbody>
        {rows}
    </tbody>
</table>
<p><a href="index.html">Back to index</a></p>
</div>
</body>
</html>
"""

# Template for the about page
ABOUT_TEMPLATE = """
<!DOCTYPE html>
<html lang='en'>
<head>
    <meta charset='UTF-8'>
    <title>About - Endolla Watcher</title>
    <link href="https://cdn.jsdelivr.net/npm/bootswatch@5.3.2/dist/flatly/bootstrap.min.css" rel="stylesheet">
</head>
<body>
{navbar}
<div class="container py-4">
<h1>About Endolla Watcher</h1>
<p>Endolla Watcher keeps an eye on Barcelona's public charging network. It highlights stations that appear inactive or unavailable so issues can be resolved quickly.</p>
<p>This project is built and maintained by <strong>dewgenenny</strong>, an electric vehicle and data enthusiast eager to optimise infrastructure through better information.</p>
</div>
</body>
</html>
"""


def render(
    problematic: List[Dict[str, Any]],
    stats: Dict[str, float] | None = None,
    history: List[Dict[str, Any]] | None = None,
    updated: str | None = None,
    db_size: float | None = None,
    elapsed: float | None = None,
) -> str:
    """Return the HTML for the main report page."""
    logger.debug("Rendering %d problematic ports", len(problematic))
    if stats is None:
        stats = {
            "chargers": 0,
            "unavailable": 0,
            "charging": 0,
            "sessions": 0,
            "short_sessions": 0,
            "avg_session_min": 0.0,
        }
    history_js = ""
    if history:
        history_js = (
            "const historyData = "
            + json.dumps(history)
            + ";\n"
            + "const labels = historyData.map(d => d.ts);\n"
            + "const ctx = document.getElementById('historyChart').getContext('2d');\n"
            + "new Chart(ctx, {type: 'line', data: {labels, datasets: ["
            + "{label: 'Unavailable', data: historyData.map(d => d.unavailable),"
            + "borderColor: '#dc3545', backgroundColor: 'rgba(220,53,69,0.3)', fill: true, stack: 'usage'},"
            + "{label: 'Charging', data: historyData.map(d => d.charging),"
            + "borderColor: '#198754', backgroundColor: 'rgba(25,135,84,0.3)', fill: true, stack: 'usage'},"
            + "{label: 'Total', data: historyData.map(d => d.chargers),"
            + "borderColor: '#0d6efd', backgroundColor: 'rgba(13,110,253,0.3)', fill: false}]},"
            + "options: {scales: {x: {type: 'time', time: {unit: 'day'}}, y: {beginAtZero: true, stacked: true}}}});"
        )
    rows = []
    for r in problematic:
        loc = r.get("location_id") or ""
        sta = r.get("station_id") or ""
        url = f"charger_{loc}_{sta}.html"
        cells = [
            f"<td><a href='{url}'>{loc}</a></td>",
            f"<td><a href='{url}'>{sta}</a></td>",
            f"<td>{r.get('port_id','')}</td>",
            f"<td>{r.get('status','')}</td>",
            f"<td>{r.get('reason','')}</td>",
        ]
        row = "<tr>" + "".join(cells) + "</tr>"
        rows.append(row)
    html = INDEX_TEMPLATE.format(
        rows="\n".join(rows),
        navbar=NAVBAR,
        history_js=history_js,
        updated=updated or "N/A",
        db_size=(db_size if db_size is not None else 0.0),
        elapsed=(elapsed if elapsed is not None else 0.0),
        **stats,
    )
    logger.debug("Generated HTML with %d rows", len(rows))
    return html


def render_about() -> str:
    """Return the HTML for the about page."""
    return ABOUT_TEMPLATE.format(navbar=NAVBAR)


def render_charger(
    location_id: str | None,
    station_id: str | None,
    sessions: Dict[str | None, List[Dict[str, Any]]],
) -> str:
    """Return HTML for a single charger with its recent sessions."""
    rows: List[str] = []
    for port, items in sessions.items():
        for s in items:
            row = (
                "<tr>"
                f"<td>{port or ''}</td>"
                f"<td>{s['start']}</td>"
                f"<td>{s['end']}</td>"
                f"<td>{s['duration']:.1f}</td>"
                "</tr>"
            )
            rows.append(row)
    html = CHARGER_TEMPLATE.format(
        navbar=NAVBAR,
        station_id=station_id or '',
        rows="\n".join(rows),
    )
    logger.debug(
        "Generated charger page for %s/%s with %d rows",
        location_id,
        station_id,
        len(rows),
    )
    return html
