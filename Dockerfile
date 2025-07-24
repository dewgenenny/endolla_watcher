FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt pyproject.toml ./

# Install git so the push_site.py helper can run inside the container
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf /var/lib/apt/lists/*

COPY src ./src
COPY scripts/push_site.py /usr/local/bin/push_site.py
RUN chmod +x /usr/local/bin/push_site.py && \
    pip install .
# Continuously fetch the dataset and render the site
# Always write the dataset and generated site under /data so volumes can
# persist them. Additional arguments provided to `docker run` will be appended
# allowing the defaults here to be overridden if needed.
ENTRYPOINT ["python", "-m", "endolla_watcher.loop", "--file", "/data/endolla.json", "--output", "/data/site/index.html", "--db", "/data/endolla.db"]
# Default intervals can be replaced by passing arguments when running the
# container.
CMD ["--fetch-interval", "300", "--update-interval", "3600"]
