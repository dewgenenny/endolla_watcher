FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
RUN pip install .

EXPOSE 8000

CMD ["uvicorn", "endolla_watcher.api:app", "--host", "0.0.0.0", "--port", "8000"]
