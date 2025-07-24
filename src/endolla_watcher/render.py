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
    stats: Dict[str, int] | None = None,
    history: List[Dict[str, Any]] | None = None,
) -> str:
    """Return the HTML for the main report page."""
    logger.debug("Rendering %d problematic ports", len(problematic))
    if stats is None:
        stats = {"chargers": 0, "unavailable": 0, "charging": 0, "sessions": 0}
    history_js = ""
    if history:
        history_js = (
            "const historyData = "
            + json.dumps(history)
            + ";\n"
            + "const labels = historyData.map(d => new Date(d.ts).toLocaleString());\n"
            + "const ctx = document.getElementById('historyChart').getContext('2d');\n"
            + "new Chart(ctx, {type: 'line', data: {labels, datasets: ["
            + "{label: 'Unavailable', data: historyData.map(d => d.unavailable),"
            + "borderColor: '#dc3545', backgroundColor: 'rgba(220,53,69,0.3)', fill: true, stack: 'usage'},"
            + "{label: 'Charging', data: historyData.map(d => d.charging),"
            + "borderColor: '#198754', backgroundColor: 'rgba(25,135,84,0.3)', fill: true, stack: 'usage'},"
            + "{label: 'Total', data: historyData.map(d => d.chargers),"
            + "borderColor: '#0d6efd', backgroundColor: 'rgba(13,110,253,0.3)', fill: false}]},"
            + "options: {scales: {y: {beginAtZero: true, stacked: true}}}});"
        )
    rows = []
    for r in problematic:
        row = "<tr>" + "".join(
            f"<td>{r.get(k,'')}</td>" for k in ["location_id", "station_id", "port_id", "status", "reason"]
        ) + "</tr>"
        rows.append(row)
    html = INDEX_TEMPLATE.format(
        rows="\n".join(rows), navbar=NAVBAR, history_js=history_js, **stats
    )
    logger.debug("Generated HTML with %d rows", len(rows))
    return html


def render_about() -> str:
    """Return the HTML for the about page."""
    return ABOUT_TEMPLATE.format(navbar=NAVBAR)
