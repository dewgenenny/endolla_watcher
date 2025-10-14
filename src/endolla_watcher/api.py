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
from pymysql.err import OperationalError

from .data import fetch_locations
from .logging_utils import setup_logging
from .loop import fetch_once
from .rules import Rules
from . import storage

logger = logging.getLogger(__name__)

DASHBOARD_WARM_INTERVAL = 15 * 60


@dataclass
class Settings:
    """Runtime configuration for the backend service."""

    db_url: str
    dataset_file: Path | None
    fetch_interval: int
    auto_fetch: bool
    location_file: Path | None
    rules: Rules
    cors_origins: list[str]
    debug: bool
    dashboard_cache_ttl: int
    dashboard_cache_presets: list[tuple[int, str]]


def _parse_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _parse_cache_presets(value: Optional[str]) -> list[tuple[int, str]]:
    """Convert preset definitions to (days, granularity) pairs."""

    default = [(5, "hour"), (5, "day")]
    if value is None:
        return default

    presets: list[tuple[int, str]] = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" not in item:
            logger.warning("Ignoring invalid dashboard cache preset '%s'", item)
            continue
        days_raw, granularity_raw = item.split(":", 1)
        try:
            days_value = int(days_raw.strip())
        except ValueError:
            logger.warning(
                "Ignoring dashboard cache preset '%s' with non-integer days", item
            )
            continue
        granularity_value = granularity_raw.strip().lower()
        if granularity_value not in {"day", "hour"}:
            logger.warning(
                "Ignoring dashboard cache preset '%s' with unsupported granularity",
                item,
            )
            continue
        presets.append((days_value, granularity_value))

    return presets or default


def _cache_presets(settings: Settings) -> list[tuple[int, str]]:
    """Return the unique dashboard cache presets with required defaults."""

    def _normalise(preset: tuple[int, str]) -> tuple[int, str]:
        days, granularity = preset
        return int(days), granularity.lower()

    presets = [_normalise(preset) for preset in settings.dashboard_cache_presets]
    required = [_normalise((5, "hour")), _normalise((5, "day"))]
    for preset in required:
        if preset not in presets:
            presets.append(preset)

    seen: set[tuple[int, str]] = set()
    ordered: list[tuple[int, str]] = []
    for preset in presets:
        if preset in seen:
            continue
        seen.add(preset)
        ordered.append(preset)
    return ordered


def load_settings() -> Settings:
    """Load backend configuration from environment variables."""

    db_url = os.getenv("ENDOLLA_DB_URL")
    if not db_url:
        raise RuntimeError("ENDOLLA_DB_URL must be configured")
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
    dashboard_cache_ttl = int(os.getenv("ENDOLLA_DASHBOARD_CACHE_TTL", "60"))
    dashboard_cache_presets = _parse_cache_presets(
        os.getenv("ENDOLLA_DASHBOARD_CACHE_PRESETS")
    )

    return Settings(
        db_url=db_url,
        dataset_file=dataset_file,
        fetch_interval=fetch_interval,
        auto_fetch=auto_fetch,
        location_file=location_file,
        rules=rules,
        cors_origins=cors_origins or ["*"],
        debug=debug,
        dashboard_cache_ttl=dashboard_cache_ttl,
        dashboard_cache_presets=dashboard_cache_presets,
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
        changed = await asyncio.to_thread(
            fetch_once, settings.db_url, settings.dataset_file
        )
        now_iso = datetime.now().astimezone().isoformat(timespec="seconds")
        app.state.last_fetch = now_iso
        if changed:
            logger.info("Dataset changed; refreshing cached dashboard data")
            app.state.last_data_update = now_iso
            await _handle_data_refresh(settings)
        else:
            logger.debug("No new data detected; cached dashboard data is still valid")
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
    try:
        return storage.connect(settings.db_url)
    except OperationalError as exc:
        logger.exception("Failed to connect to MySQL")
        raise HTTPException(
            status_code=503,
            detail=(
                "Unable to connect to the Endolla database. "
                "Verify that the MySQL service is reachable and credentials are valid."
            ),
        ) from exc
    except RuntimeError as exc:
        # PyMySQL raises RuntimeError when optional auth dependencies are missing.
        logger.exception("MySQL initialisation failed")
        message = "Database initialisation failed."
        if "cryptography" in str(exc).lower():
            message = (
                "Database initialisation failed because the 'cryptography' package is missing. "
                "Install it to support caching_sha2_password authentication."
            )
        raise HTTPException(status_code=500, detail=message) from exc


def _latest_snapshot(conn) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(ts) FROM port_status")
        row = cur.fetchone()
    if row and row[0]:
        return str(row[0])
    return None


async def _clear_dashboard_cache() -> None:
    cache: Dict[Any, Dict[str, Any]] | None = getattr(app.state, "dashboard_cache", None)
    if cache is None:
        return
    lock: asyncio.Lock | None = getattr(app.state, "dashboard_cache_lock", None)
    if lock is not None:
        async with lock:
            cache.clear()
    else:  # pragma: no cover - startup initialises the lock
        cache.clear()


async def _handle_data_refresh(settings: Settings) -> None:
    app.state.dashboard_version = getattr(app.state, "dashboard_version", 0) + 1
    await _clear_dashboard_cache()
    await _warm_dashboard_cache(settings)


async def _warm_dashboard_cache(settings: Settings) -> None:
    cache: Dict[Any, Dict[str, Any]] | None = getattr(app.state, "dashboard_cache", None)
    if cache is None:
        return

    lock: asyncio.Lock | None = getattr(app.state, "dashboard_cache_lock", None)
    locations: Dict[str, Dict[str, float]] = getattr(app.state, "locations", {})

    presets = _cache_presets(settings)
    version = getattr(app.state, "dashboard_version", 0)

    for days, granularity in presets:
        try:
            data = await asyncio.to_thread(
                _build_dashboard, settings, locations, days, granularity
            )
        except Exception:  # pragma: no cover - defensive logging
            logger.exception(
                "Failed to warm dashboard cache for days=%s granularity=%s",
                days,
                granularity,
            )
            continue

        entry = {"data": data, "version": version}
        if lock is not None:
            async with lock:
                cache[(days, granularity)] = entry
        else:  # pragma: no cover - startup initialises the lock
            cache[(days, granularity)] = entry


async def _dashboard_warm_loop(settings: Settings, interval: int) -> None:
    interval_seconds = max(interval, 1)
    logger.info(
        "Starting dashboard warm loop with interval %ss", interval_seconds
    )
    try:
        while True:
            try:
                await _warm_dashboard_cache(settings)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Failed to warm dashboard cache")
            await asyncio.sleep(interval_seconds)
    except asyncio.CancelledError:  # pragma: no cover - shutdown cleanup
        logger.debug("Dashboard warm loop cancelled")
        raise


def _build_dashboard(
    settings: Settings,
    locations: Dict[str, Dict[str, float]],
    daily_days: int,
    granularity: str,
) -> Dict[str, Any]:
    conn = _connect_db(settings)
    try:
        problematic, rule_counts = storage.analyze_chargers(conn, settings.rules)
        stats = storage.stats_from_db(conn)
        history = storage.timeline_stats(conn, settings.rules)
        daily = storage.sessions_per_day(conn, days=daily_days)
        series = storage.sessions_time_series(conn, days=daily_days, granularity=granularity)
        db_stats = storage.db_stats(conn)
        updated = _latest_snapshot(conn)
    finally:
        conn.close()

    return {
        "problematic": problematic,
        "stats": stats,
        "history": history,
        "daily": daily,
        "series": series,
        "series_granularity": granularity,
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
    # MySQL connections do not require local directories to exist.
    app.state.settings = settings
    if settings.cors_origins != _INITIAL_SETTINGS.cors_origins:
        logger.warning(
            "CORS origin configuration changed to %s after startup; restart required for changes to apply.",
            settings.cors_origins,
        )
    app.state.locations = await _load_locations(settings)
    app.state.fetch_task = None
    app.state.dashboard_warm_task = None
    app.state.last_fetch = None
    app.state.dashboard_cache: Dict[Any, Dict[str, Any]] = {}
    app.state.dashboard_cache_lock = asyncio.Lock()
    app.state.dashboard_version = 0
    app.state.last_data_update = None
    if settings.auto_fetch:
        await _fetch_once(settings)
    await _warm_dashboard_cache(settings)
    if settings.auto_fetch:
        app.state.fetch_task = asyncio.create_task(_fetch_loop(settings))
    app.state.dashboard_warm_task = asyncio.create_task(
        _dashboard_warm_loop(settings, DASHBOARD_WARM_INTERVAL)
    )


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
    warm_task: asyncio.Task | None = getattr(app.state, "dashboard_warm_task", None)
    if warm_task is not None:
        warm_task.cancel()
        try:
            await warm_task
        except asyncio.CancelledError:
            pass
        finally:
            app.state.dashboard_warm_task = None


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
async def dashboard(
    days: int = Query(5, ge=1, le=90),
    granularity: str = Query("day"),
) -> Dict[str, Any]:
    settings = _require_settings()
    locations: Dict[str, Dict[str, float]] = getattr(app.state, "locations", {})
    granularity_normalized = granularity.lower()
    if granularity_normalized not in {"day", "hour"}:
        raise HTTPException(status_code=422, detail="Unsupported granularity")
    cache_key = (days, granularity_normalized)
    cache: Dict[Any, Dict[str, Any]] | None = getattr(app.state, "dashboard_cache", None)
    lock: asyncio.Lock | None = getattr(app.state, "dashboard_cache_lock", None)
    version = getattr(app.state, "dashboard_version", 0)
    if cache is not None:
        cached = cache.get(cache_key)
        if cached and cached.get("version") == version:
            return cached["data"]
    data = await asyncio.to_thread(
        _build_dashboard, settings, locations, days, granularity_normalized
    )
    if cache is not None:
        entry = {"data": data, "version": version}
        if lock is not None:
            async with lock:
                cache[cache_key] = entry
        else:  # pragma: no cover - startup initialises the lock
            cache[cache_key] = entry
    return data


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


@app.get("/api/locations/{location_id}")
async def location_details(location_id: str) -> Dict[str, Any]:
    settings = _require_settings()
    locations: Dict[str, Dict[str, float]] = getattr(app.state, "locations", {})

    def _load_location() -> Dict[str, Any] | None:
        conn = _connect_db(settings)
        try:
            return storage.location_usage(conn, location_id)
        finally:
            conn.close()

    details = await asyncio.to_thread(_load_location)
    if details is None:
        raise HTTPException(status_code=404, detail="Location not found or no telemetry available")

    coords = locations.get(location_id)
    if isinstance(coords, dict):
        lat = coords.get("lat")
        lon = coords.get("lon")
        coord_payload = None
        if lat is not None and lon is not None:
            coord_payload = {"lat": lat, "lon": lon}
        address_value = coords.get("address")
        address = address_value.strip() if isinstance(address_value, str) else None
        if coord_payload is not None and address:
            coord_payload["address"] = address
        details["coordinates"] = coord_payload
        if address:
            details["address"] = address
        elif "address" not in details:
            details["address"] = None
    else:
        details["coordinates"] = None
        if "address" not in details:
            details["address"] = None
    return details


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
