from typing import List, Dict, Any
from .rules import Rules
import json
import logging

logger = logging.getLogger(__name__)


# Navigation bar shared across pages
NAVBAR = """
<nav class="navbar navbar-expand-lg navbar-dark bg-primary">
  <div class="container-fluid">
    <a class="navbar-brand" href="index.html">Endolla Watcher</a>
    <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav" aria-controls="navbarNav" aria-expanded="false" aria-label="Toggle navigation">
      <span class="navbar-toggler-icon"></span>
    </button>
    <div class="collapse navbar-collapse" id="navbarNav">
      <ul class="navbar-nav ms-auto">
        <li class="nav-item"><a class="nav-link" href="problematic.html">Problematic</a></li>
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
    <li class="list-group-item">Total ports: {chargers}</li>
    <li class="list-group-item">Unavailable ports: {unavailable}</li>
    <li class="list-group-item">Currently charging: {charging}</li>
    <li class="list-group-item">Total charging events: {sessions}</li>
    <li class="list-group-item">Charges today: {charges_today}</li>
    <li class="list-group-item">Short sessions (<5 min): {short_sessions}</li>
    <li class="list-group-item">Avg session (24h): {avg_session_min:.1f} min</li>
</ul>
<h2 class="mt-4">Rule Summary</h2>
<ul class="list-group mb-4">
    <li class="list-group-item">Unused > {unused_days}d: {unused}</li>
    <li class="list-group-item">No session >= {long_session_min}min in {long_session_days}d: {no_long}</li>
    <li class="list-group-item">Unavailable > {unavailable_hours}h: {unavailable_rule}</li>
</ul>
<div class="mb-4">
    <canvas id="unusedChart" height="60"></canvas>
</div>
<div class="mb-4">
    <canvas id="chargesChart" height="60"></canvas>
</div>
<div class="mb-4">
    <canvas id="problematicChart" height="60"></canvas>
</div>
<div class="mb-4">
    <canvas id="chargingChart" height="60"></canvas>
</div>
<script>
{history_js}
</script>
<h2 class="mt-4">Problematic Chargers</h2>
<p><a href="problematic.html">View {problematic_count} chargers</a></p>
<div class="text-muted small mt-4">
    <p>Page last updated: {updated}</p>
    <p>DB size: {db_size:.1f} MB</p>
    <p>Processed in {elapsed:.2f} s</p>
</div>
</div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
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
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

# Template for a separate page listing problematic chargers
PROBLEMATIC_TEMPLATE = """
<!DOCTYPE html>
<html lang='en'>
<head>
    <meta charset='UTF-8'>
    <title>Problematic Chargers</title>
    <link href="https://cdn.jsdelivr.net/npm/bootswatch@5.3.2/dist/flatly/bootstrap.min.css" rel="stylesheet">
</head>
<body>
{navbar}
<div class="container py-4">
<h1 class="mb-4">Problematic Chargers</h1>
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
<p><a href="index.html">Back to index</a></p>
<div class="text-muted small mt-4">
    <p>Page last updated: {updated}</p>
    <p>DB size: {db_size:.1f} MB</p>
    <p>Processed in {elapsed:.2f} s</p>
</div>
</div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
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
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""


def render(
    problematic: List[Dict[str, Any]],
    stats: Dict[str, Any] | None = None,
    history: List[Dict[str, Any]] | None = None,
    daily: List[Dict[str, Any]] | None = None,
    rule_counts: Dict[str, int] | None = None,
    rules: Rules | None = None,
    updated: str | None = None,
    db_size: float | None = None,
    elapsed: float | None = None,
    locations: Dict[str, Dict[str, float]] | None = None,
) -> str:
    """Return the HTML for the main report page."""
    logger.debug("Rendering %d problematic ports", len(problematic))
    if stats is None:
        stats = {
            "chargers": 0,
            "unavailable": 0,
            "charging": 0,
            "sessions": 0,
            "charges_today": 0,
            "short_sessions": 0,
            "avg_session_min": 0.0,
            "utilization": {
                "ports": [],
                "stations": [],
                "locations": [],
                "network": {},
            },
        }
    if rule_counts is None:
        rule_counts = {"unused": 0, "no_long": 0, "unavailable": 0}
    history_js = ""
    if history:
        history_js += "const historyData = " + json.dumps(history) + "\n"
        history_js += "const labels = historyData.map(d => d.ts);\n"
        history_js += (
            "new Chart(document.getElementById('unusedChart').getContext('2d'), {"+
            "type: 'line', data: {labels, datasets: ["+
            "{label: 'Unused >1d', data: historyData.map(d => d.unused_1),"+
            "borderColor: '#ffc107', backgroundColor: 'rgba(255,193,7,0.3)', fill: true},"+
            "{label: 'Unused >2d', data: historyData.map(d => d.unused_2),"+
            "borderColor: '#6f42c1', backgroundColor: 'rgba(111,66,193,0.3)', fill: true},"+
            "{label: 'Unused >7d', data: historyData.map(d => d.unused_7),"+
            "borderColor: '#0d6efd', backgroundColor: 'rgba(13,110,253,0.3)', fill: true}]},"+
            "options: {scales: {x: {type: 'time', time: {unit: 'day'}}, y: {beginAtZero: true}}}});\n"
        )
        history_js += (
            "new Chart(document.getElementById('problematicChart').getContext('2d'), {"+
            "type: 'line', data: {labels, datasets: ["+
            "{label: 'Problematic', data: historyData.map(d => d.problematic),"+
            "borderColor: '#fd7e14', backgroundColor: 'rgba(253,126,20,0.3)', fill: true}]},"+
            "options: {scales: {x: {type: 'time', time: {unit: 'day'}}, y: {beginAtZero: true}}}});\n"
        )
        history_js += (
            "new Chart(document.getElementById('chargingChart').getContext('2d'), {"+
            "type: 'line', data: {labels, datasets: ["+
            "{label: 'Charging', data: historyData.map(d => d.charging),"+
            "borderColor: '#198754', backgroundColor: 'rgba(25,135,84,0.3)', fill: true}]},"+
            "options: {scales: {x: {type: 'time', time: {unit: 'day'}}, y: {beginAtZero: true}}}});\n"
        )
    if daily:
        history_js += "const dailyData = " + json.dumps(daily) + "\n"
        history_js += "const dailyLabels = dailyData.map(d => d.day);\n"
        history_js += (
            "new Chart(document.getElementById('chargesChart').getContext('2d'), {"+
            "type: 'bar', data: {labels: dailyLabels, datasets: ["+
            "{label: 'Charges', data: dailyData.map(d => d.sessions),"+
            "backgroundColor: '#0d6efd'}]},"+
            "options: {scales: {y: {beginAtZero: true}}}});\n"
        )
    html = INDEX_TEMPLATE.format(
        navbar=NAVBAR,
        history_js=history_js,
        problematic_count=len(problematic),
        updated=updated or "N/A",
        db_size=(db_size if db_size is not None else 0.0),
        elapsed=(elapsed if elapsed is not None else 0.0),
        unused=rule_counts.get("unused", 0),
        no_long=rule_counts.get("no_long", 0),
        unavailable_rule=rule_counts.get("unavailable", 0),
        unused_days=(rules.unused_days if rules else 0),
        long_session_min=(rules.long_session_min if rules else 0),
        long_session_days=(rules.long_session_days if rules else 0),
        unavailable_hours=(rules.unavailable_hours if rules else 0),
        **stats,
    )
    logger.debug("Generated index HTML with %d problematic entries", len(problematic))
    return html


def render_about() -> str:
    """Return the HTML for the about page."""
    return ABOUT_TEMPLATE.format(navbar=NAVBAR)


def _render_problematic_rows(
    entries: List[Dict[str, Any]],
    locations: Dict[str, Dict[str, float]] | None = None,
) -> str:
    """Return HTML table rows for problematic chargers."""
    rows: List[str] = []
    for r in entries:
        loc = r.get("location_id") or ""
        sta = r.get("station_id") or ""
        url = f"charger_{loc}_{sta}.html"
        latlon = locations.get(loc) if locations else None
        if latlon:
            map_url = f"https://www.google.com/maps?q={latlon['lat']},{latlon['lon']}"
            loc_cell = f"<td><a href='{map_url}' target='_blank'>{loc}</a></td>"
        else:
            loc_cell = f"<td>{loc}</td>"
        cells = [
            loc_cell,
            f"<td><a href='{url}'>{sta}</a></td>",
            f"<td>{r.get('port_id','')}</td>",
            f"<td>{r.get('status','')}</td>",
            f"<td>{r.get('reason','')}</td>",
        ]
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return "\n".join(rows)


def render_problematic(
    problematic: List[Dict[str, Any]],
    updated: str | None = None,
    db_size: float | None = None,
    elapsed: float | None = None,
    locations: Dict[str, Dict[str, float]] | None = None,
) -> str:
    """Return the HTML page listing problematic chargers."""
    rows = _render_problematic_rows(problematic, locations)
    html = PROBLEMATIC_TEMPLATE.format(
        navbar=NAVBAR,
        rows=rows,
        updated=updated or "N/A",
        db_size=(db_size if db_size is not None else 0.0),
        elapsed=(elapsed if elapsed is not None else 0.0),
    )
    logger.debug("Generated problematic page with %d rows", len(problematic))
    return html


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
