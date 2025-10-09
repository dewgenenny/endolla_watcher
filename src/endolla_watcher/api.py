"""FastAPI backend exposing Endolla Watcher data."""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from .data import fetch_locations
from .logging_utils import setup_logging
from .loop import fetch_once
from .rules import Rules
from . import storage

logger = logging.getLogger(__name__)


@dataclass
class Settings:
    """Runtime configuration for the backend service."""

    db_path: Path
    dataset_file: Path | None
    fetch_interval: int
    auto_fetch: bool
    location_file: Path | None
    rules: Rules
    cors_origins: list[str]
    debug: bool


def _parse_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def load_settings() -> Settings:
    """Load backend configuration from environment variables."""

    db_path = Path(os.getenv("ENDOLLA_DB_PATH", "/data/endolla.db"))
    dataset_file_env = os.getenv("ENDOLLA_DATA_FILE")
    dataset_file = Path(dataset_file_env) if dataset_file_env else None
    fetch_interval = int(os.getenv("ENDOLLA_FETCH_INTERVAL", "300"))
    auto_fetch = _parse_bool(os.getenv("ENDOLLA_AUTO_FETCH", "1"), True)
    location_file_env = os.getenv("ENDOLLA_LOCATIONS_FILE")
    location_file = Path(location_file_env) if location_file_env else None

    default_rules = Rules()
    rules = Rules(
        unused_days=int(
            os.getenv("ENDOLLA_RULE_UNUSED_DAYS", str(default_rules.unused_days))
        ),
        long_session_days=int(
            os.getenv("ENDOLLA_RULE_LONG_DAYS", str(default_rules.long_session_days))
        ),
        long_session_min=int(
            os.getenv("ENDOLLA_RULE_LONG_MIN", str(default_rules.long_session_min))
        ),
        unavailable_hours=int(
            os.getenv(
                "ENDOLLA_RULE_UNAVAILABLE_HOURS", str(default_rules.unavailable_hours)
            )
        ),
    )

    cors_env = os.getenv("ENDOLLA_CORS_ORIGINS", "*")
    cors_origins = [origin.strip() for origin in cors_env.split(",") if origin.strip()]
    debug = _parse_bool(os.getenv("ENDOLLA_DEBUG"), False)

    return Settings(
        db_path=db_path,
        dataset_file=dataset_file,
        fetch_interval=fetch_interval,
        auto_fetch=auto_fetch,
        location_file=location_file,
        rules=rules,
        cors_origins=cors_origins or ["*"],
        debug=debug,
    )

_INITIAL_SETTINGS = load_settings()

app = FastAPI(title="Endolla Watcher API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_INITIAL_SETTINGS.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)


async def _load_locations(settings: Settings) -> Dict[str, Dict[str, float]]:
    try:
        return await asyncio.to_thread(fetch_locations, settings.location_file)
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Failed to load charger location data")
        return {}


async def _fetch_once(settings: Settings) -> None:
    try:
        await asyncio.to_thread(fetch_once, settings.db_path, settings.dataset_file)
        app.state.last_fetch = datetime.now().astimezone().isoformat(timespec="seconds")
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Dataset fetch failed")


async def _fetch_loop(settings: Settings) -> None:
    logger.info(
        "Starting fetch loop with interval %ss (auto_fetch=%s)",
        settings.fetch_interval,
        settings.auto_fetch,
    )
    while True:
        await _fetch_once(settings)
        await asyncio.sleep(max(settings.fetch_interval, 1))


def _connect_db(settings: Settings):
    return storage.connect(settings.db_path)


def _latest_snapshot(conn) -> str | None:
    row = conn.execute("SELECT MAX(ts) FROM port_status").fetchone()
    if row and row[0]:
        return str(row[0])
    return None


def _build_dashboard(
    settings: Settings,
    locations: Dict[str, Dict[str, float]],
    daily_days: int,
) -> Dict[str, Any]:
    conn = _connect_db(settings)
    try:
        problematic, rule_counts = storage.analyze_chargers(conn, settings.rules)
        stats = storage.stats_from_db(conn)
        history = storage.timeline_stats(conn, settings.rules)
        daily = storage.sessions_per_day(conn, days=daily_days)
        db_stats = storage.db_stats(conn)
        updated = _latest_snapshot(conn)
    finally:
        conn.close()

    return {
        "problematic": problematic,
        "stats": stats,
        "history": history,
        "daily": daily,
        "rule_counts": rule_counts,
        "rules": asdict(settings.rules),
        "updated": updated,
        "db": db_stats,
        "locations": locations,
        "last_fetch": getattr(app.state, "last_fetch", None),
    }


@app.on_event("startup")
async def on_startup() -> None:
    settings = load_settings()
    setup_logging(settings.debug)
    logger.debug("Loaded settings: %s", settings)
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    app.state.settings = settings
    if settings.cors_origins != _INITIAL_SETTINGS.cors_origins:
        logger.warning(
            "CORS origin configuration changed to %s after startup; restart required for changes to apply.",
            settings.cors_origins,
        )
    app.state.locations = await _load_locations(settings)
    app.state.fetch_task = None
    app.state.last_fetch = None
    if settings.auto_fetch:
        await _fetch_once(settings)
        app.state.fetch_task = asyncio.create_task(_fetch_loop(settings))


@app.on_event("shutdown")
async def on_shutdown() -> None:
    task: asyncio.Task | None = getattr(app.state, "fetch_task", None)
    if task is not None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        finally:
            app.state.fetch_task = None


def _require_settings() -> Settings:
    settings = getattr(app.state, "settings", None)
    if settings is None:  # pragma: no cover - startup should populate
        raise HTTPException(status_code=503, detail="Service not initialised")
    return settings


@app.get("/healthz")
async def healthz() -> Dict[str, Any]:
    settings = _require_settings()
    last_fetch = getattr(app.state, "last_fetch", None)
    return {
        "status": "ok",
        "auto_fetch": settings.auto_fetch,
        "last_fetch": last_fetch,
    }


@app.get("/api/dashboard")
async def dashboard(days: int = Query(5, ge=1, le=90)) -> Dict[str, Any]:
    settings = _require_settings()
    locations: Dict[str, Dict[str, float]] = getattr(app.state, "locations", {})
    return await asyncio.to_thread(_build_dashboard, settings, locations, days)


@app.post("/api/refresh", status_code=202)
async def refresh() -> Dict[str, Any]:
    settings = _require_settings()
    await _fetch_once(settings)
    return {"status": "scheduled"}


@app.get("/api/chargers/{location_id}/{station_id}")
async def charger_details(location_id: str, station_id: str) -> Dict[str, Any]:
    settings = _require_settings()

    def _load_sessions() -> Dict[str | None, Any]:
        conn = _connect_db(settings)
        try:
            return storage.charger_sessions(conn, location_id, station_id, limit=20)
        finally:
            conn.close()

    sessions = await asyncio.to_thread(_load_sessions)
    if not any(sessions.values()):
        raise HTTPException(status_code=404, detail="Charger not found")
    return {"sessions": sessions}


@app.get("/api/locations")
async def locations() -> Dict[str, Dict[str, float]]:
    return getattr(app.state, "locations", {})


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    import uvicorn

    uvicorn.run(
        "endolla_watcher.api:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
    )
