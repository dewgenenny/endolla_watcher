FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
RUN pip install .
# Continuously fetch the dataset and render the site
ENTRYPOINT ["python", "-m", "endolla_watcher.loop"]
# Default to fetching the dataset every five minutes and updating the report
# hourly. Data and generated site files live under /data so they can be
# persisted via a volume when running the container.
CMD [
    "--file", "/data/endolla.json",
    "--output", "/data/site/index.html",
    "--db", "/data/endolla.db",
    "--fetch-interval", "300",
    "--update-interval", "3600"
]
