FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
# Continuously fetch the dataset and render the site
ENTRYPOINT ["python", "-m", "endolla_watcher.loop"]
CMD ["--file", "/data/endolla.json", "--interval", "300"]
