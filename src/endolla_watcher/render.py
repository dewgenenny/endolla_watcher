from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang='en'>
<head>
    <meta charset='UTF-8'>
    <title>Endolla Watcher</title>
    <style>
        table {{border-collapse: collapse;}}
        th, td {{border: 1px solid #ccc; padding: 4px;}}
    </style>
</head>
<body>
<h1>Statistics</h1>
<ul>
    <li>Total chargers: {chargers}</li>
    <li>Unavailable chargers: {unavailable}</li>
    <li>Currently charging: {charging}</li>
    <li>Total charging events: {sessions}</li>
</ul>
<h1>Problematic Chargers</h1>
<table>
    <thead>
        <tr><th>Location</th><th>Station</th><th>Port</th><th>Status</th><th>Reason</th></tr>
    </thead>
    <tbody>
        {rows}
    </tbody>
</table>
</body>
</html>
"""


def render(problematic: List[Dict[str, any]], stats: Dict[str, int] | None = None) -> str:
    logger.debug("Rendering %d problematic ports", len(problematic))
    if stats is None:
        stats = {"chargers": 0, "unavailable": 0, "charging": 0, "sessions": 0}
    rows = []
    for r in problematic:
        row = "<tr>" + "".join(
            f"<td>{r.get(k,'')}</td>" for k in ["location_id","station_id","port_id","status","reason"]
        ) + "</tr>"
        rows.append(row)
    html = HTML_TEMPLATE.format(rows="\n".join(rows), **stats)
    logger.debug("Generated HTML with %d rows", len(rows))
    return html
