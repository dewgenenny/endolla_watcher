const API_BASE = window.ENDOLLA_API_BASE || '/api';
const DEFAULT_DAYS = 5;
const MAX_DAYS = 90;

const yearEl = document.getElementById('year');
if (yearEl) {
  yearEl.textContent = new Date().getFullYear();
}

const chargesChartCanvas = document.getElementById('charges-chart');
const chargesStatus = document.getElementById('charges-chart-status');
const chargesTotalEl = document.getElementById('charges-total');
const chargesRangeWindow = document.getElementById('charges-range-window');
const chargesRangeControl = document.getElementById('charges-range');
const utilizationTabsRoot = document.getElementById('utilization-tabs');
const utilizationTabButtons = utilizationTabsRoot
  ? Array.from(utilizationTabsRoot.querySelectorAll('[data-tab]'))
  : [];
const utilizationPanels = document.querySelectorAll('[data-utilization-panel]');
const utilizationLocationsBody = document.getElementById('utilization-locations-body');
const utilizationStationsBody = document.getElementById('utilization-stations-body');
const utilizationPortsBody = document.getElementById('utilization-ports-body');
const utilizationSortButton = document.getElementById('utilization-sort');
const utilizationRateHeader = document.getElementById('utilization-rate-header');
const locationDetailRoot = document.getElementById('location-detail');
const locationLoadingEl = document.getElementById('location-loading');
const locationErrorEl = document.getElementById('location-error');
const locationTitleEl = document.getElementById('location-title');
const locationAddressEl = document.getElementById('location-address');
const locationUpdatedEl = document.getElementById('location-updated');
const locationStationsEl = document.getElementById('location-stations');
const locationPortsEl = document.getElementById('location-ports');
const locationOccupationDayEl = document.getElementById('location-occupation-day');
const locationActiveDayEl = document.getElementById('location-active-day');
const locationAvailabilityEl = document.getElementById('location-availability');
const locationMonitoredEl = document.getElementById('location-monitored');
const locationMapContainer = document.getElementById('location-map');
const locationMapNoteEl = document.getElementById('location-map-note');
const locationDayChartCanvas = document.getElementById('location-usage-day');
const locationWeekChartCanvas = document.getElementById('location-usage-week');
const locationDayChartStatus = document.getElementById('location-usage-day-status');
const locationWeekChartStatus = document.getElementById('location-usage-week-status');
const highChargingListEl = document.getElementById('high-charging-list');
const highChargingNoteEl = document.getElementById('high-charging-note');
const heatmapMapContainer = document.getElementById('utilization-heatmap');
const heatmapStatusEl = document.getElementById('heatmap-status');
const heatmapMetricSelect = document.getElementById('heatmap-metric');
const heatmapLegendMinEl = document.getElementById('heatmap-legend-min');
const heatmapLegendMaxEl = document.getElementById('heatmap-legend-max');
const heatmapDescriptionEl = document.getElementById('heatmap-description');
const heatmapNoteEl = document.getElementById('heatmap-note');
const heatmapListContainer = document.getElementById('heatmap-location-list');
const heatmapUpdatedEl = document.getElementById('heatmap-updated');
const stationHeatmapSection = document.getElementById('station-heatmaps');
const stationHeatmapContainer = document.getElementById('station-heatmap-container');
const stationHeatmapStatus = document.getElementById('station-heatmap-status');
const nearMeSection = document.getElementById('near-me');
const nearMeLocateButton = document.getElementById('near-me-locate');
const nearMeStatusEl = document.getElementById('near-me-status');
const nearMeErrorEl = document.getElementById('near-me-error');
const nearMeResultsEl = document.getElementById('near-me-results');
const nearMeHighPowerOnlyInput = document.getElementById('near-me-high-power-only');
const nearMeIncludeMotorcycleInput = document.getElementById('near-me-include-motorcycle');

let chargesChart;
let dashboardController;
let activeUtilizationTabId = null;
let utilizationLocationRows = null;
let utilizationStationRows = null;
let utilizationPortRows = null;
let utilizationSortDescending = true;
let locationDayChart;
let locationWeekChart;
let locationMap;
let locationMapMarker;
let locationMapMarkerElement;
let locationMapResizeHandle;
let locationMapResizeObserver;
let locationMapResizeTimer;
let locationMapResizeTimerType;
let locationMapResizeRepeatCount = 0;
let locationMapReady = false;
let locationMapPendingCoords = null;
let heatmapMap;
let heatmapDataRows = null;
let heatmapCoordinateLookup = null;
let heatmapMetricKey = 'occupation_utilization_pct';
let heatmapResizeObserver;
let heatmapResizeHandle;
let heatmapPendingBounds;
let nearMeRequestInFlight = false;
let nearMeAutoRequested = false;
const stationHeatmapCards = new Map();
let stationHeatmapSequence = 0;
let stationHeatmapPending = 0;

const debugFlags = (() => {
  try {
    const params = new URLSearchParams(window.location.search);
    const flags = new Set();
    params.getAll('debug').forEach((value) => {
      value
        .split(',')
        .map((flag) => flag.trim().toLowerCase())
        .filter(Boolean)
        .forEach((flag) => flags.add(flag));
    });
    if (params.has('debugMap')) {
      flags.add('map');
    }
    return flags;
  } catch (error) {
    console.warn('Failed to parse debug query parameters', error);
    return new Set();
  }
})();

const isDebugEnabled = (flag) => debugFlags.has('all') || debugFlags.has(flag);

const debugLog = (flag, message, detail) => {
  if (!isDebugEnabled(flag)) {
    return;
  }
  if (detail !== undefined) {
    console.debug(`[debug:${flag}] ${message}`, detail);
  } else {
    console.debug(`[debug:${flag}] ${message}`);
  }
};

const runInNextFrame = (callback) => {
  if (typeof requestAnimationFrame === 'function') {
    requestAnimationFrame(callback);
  } else {
    setTimeout(callback, 0);
  }
};

const UTILIZATION_VIEW_IDS = ['locations', 'stations', 'ports'];
const UTILIZATION_PAGE_SIZES = [10, 25, 100];
const UTILIZATION_DEFAULT_PAGE_SIZE = UTILIZATION_PAGE_SIZES[0];
const HEATMAP_METRICS = {
  occupation_utilization_pct: {
    label: 'Occupied time',
    description: 'Share of monitored time that ports at each location were occupied.',
  },
  active_charging_utilization_pct: {
    label: 'Active charging',
    description: 'Share of monitored time spent actively charging vehicles.',
  },
};
const HEATMAP_DEFAULT_METRIC = 'occupation_utilization_pct';
const HEATMAP_COLOR_STOPS = [
  { stop: 0, h: 210, s: 85, l: 55 },
  { stop: 0.5, h: 120, s: 85, l: 50 },
  { stop: 1, h: 0, s: 85, l: 45 },
];
const NEAR_ME_DEFAULT_LIMIT = 3;

const FINGERPRINT_WEEKDAY_LABELS = [
  'Monday',
  'Tuesday',
  'Wednesday',
  'Thursday',
  'Friday',
  'Saturday',
  'Sunday',
];
const FINGERPRINT_WEEKDAY_SHORT = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const FINGERPRINT_HOUR_LABELS = Array.from({ length: 24 }, (_value, index) => `${index.toString().padStart(2, '0')}:00`);

const MAP_STYLE_URL = 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json';
const MAP_DEFAULT_CENTER = [2.1734, 41.3851];
const MAP_DEFAULT_ZOOM = 12;
const MAP_ATTRIBUTION = '© OpenStreetMap contributors, © CARTO';
const HEATMAP_SOURCE_ID = 'utilization-heatmap';
const HEATMAP_HEAT_LAYER_ID = 'utilization-heatmap-heat';
const HEATMAP_CIRCLE_LAYER_ID = 'utilization-heatmap-points';
const HEATMAP_MAX_POINT_ZOOM = 14;

let heatmapMapReady = false;
let heatmapLatestEntries = [];
let heatmapPopup;

const utilizationViewState = new Map();
const utilizationLimitControls = new Map();
const utilizationFooterContainers = new Map();

const getUtilizationViewState = (view) => {
  if (!UTILIZATION_VIEW_IDS.includes(view)) {
    return null;
  }
  if (!utilizationViewState.has(view)) {
    utilizationViewState.set(view, {
      limit: UTILIZATION_DEFAULT_PAGE_SIZE,
      visible: UTILIZATION_DEFAULT_PAGE_SIZE,
    });
  }
  return utilizationViewState.get(view);
};

const parseUtilizationPageSize = (value) => {
  const numeric = Number.parseInt(value, 10);
  if (UTILIZATION_PAGE_SIZES.includes(numeric)) {
    return numeric;
  }
  return UTILIZATION_DEFAULT_PAGE_SIZE;
};

const getUtilizationRows = (view) => {
  if (view === 'locations') {
    return Array.isArray(utilizationLocationRows) ? utilizationLocationRows : [];
  }
  if (view === 'stations') {
    return Array.isArray(utilizationStationRows) ? utilizationStationRows : [];
  }
  if (view === 'ports') {
    return Array.isArray(utilizationPortRows) ? utilizationPortRows : [];
  }
  return [];
};

const getUtilizationRowCount = (view) => getUtilizationRows(view).length;

const resetUtilizationView = (view) => {
  const state = getUtilizationViewState(view);
  if (!state) {
    return;
  }
  const totalRows = getUtilizationRowCount(view);
  if (totalRows <= 0) {
    state.visible = 0;
    return;
  }
  const limit = Math.max(1, Number(state.limit) || UTILIZATION_DEFAULT_PAGE_SIZE);
  state.visible = Math.min(limit, totalRows);
};

const ensureVisibleRowCount = (view, totalRows) => {
  const state = getUtilizationViewState(view);
  if (!state) {
    return 0;
  }
  if (totalRows <= 0) {
    state.visible = 0;
    return 0;
  }
  const limit = Math.max(1, Number(state.limit) || UTILIZATION_DEFAULT_PAGE_SIZE);
  if (!Number.isFinite(state.visible) || state.visible <= 0) {
    state.visible = limit;
  }
  state.visible = Math.min(Math.max(state.visible, limit), totalRows);
  return state.visible;
};

const clearUtilizationFooter = (view) => {
  const footer = utilizationFooterContainers.get(view);
  if (!footer) {
    return;
  }
  footer.innerHTML = '';
  footer.hidden = true;
};

const updateUtilizationFooter = (view, totalRows, visibleRows) => {
  const footer = utilizationFooterContainers.get(view);
  if (!footer) {
    return;
  }
  if (!Number.isFinite(totalRows) || totalRows <= 0) {
    clearUtilizationFooter(view);
    return;
  }

  const state = getUtilizationViewState(view);
  const limit = Math.max(1, Number(state?.limit) || UTILIZATION_DEFAULT_PAGE_SIZE);
  const remaining = Math.max(totalRows - visibleRows, 0);
  footer.hidden = false;
  footer.innerHTML = '';

  const summary = document.createElement('p');
  summary.className = 'utilization-footer-summary';
  summary.textContent =
    visibleRows < totalRows
      ? `Showing top ${formatNumber(visibleRows)} of ${formatNumber(totalRows)} results.`
      : `Showing all ${formatNumber(totalRows)} results.`;
  footer.appendChild(summary);

  if (remaining > 0) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'utilization-footer-button';
    button.dataset.utilizationMore = view;
    const increment = Math.min(limit, remaining);
    button.textContent = `Load ${formatNumber(increment)} more`;
    button.title = button.textContent;
    button.setAttribute('aria-label', `Load ${formatNumber(increment)} more ${view}`);
    footer.appendChild(button);
  }
};

const chargesGlowPlugin = {
  id: 'chargesGlow',
  beforeDatasetsDraw(chart, _args, pluginOptions) {
    if (!pluginOptions?.enabled) {
      return;
    }
    const ctx = chart.ctx;
    ctx.save();
    ctx.shadowColor = pluginOptions.color || 'rgba(14, 165, 233, 0.4)';
    ctx.shadowBlur = pluginOptions.blur ?? 25;
    ctx.shadowOffsetY = pluginOptions.offsetY ?? 16;
    ctx.shadowOffsetX = 0;
  },
  afterDatasetsDraw(chart, _args, pluginOptions) {
    if (!pluginOptions?.enabled) {
      return;
    }
    chart.ctx.restore();
  },
};

if (window.Chart) {
  window.Chart.register(chargesGlowPlugin);
}

const clampDays = (value) => {
  const parsed = Number.parseInt(value, 10);
  if (Number.isNaN(parsed)) {
    return DEFAULT_DAYS;
  }
  return Math.min(Math.max(parsed, 1), MAX_DAYS);
};

const formatNumber = (value) => {
  if (value === null || value === undefined) {
    return '–';
  }
  return new Intl.NumberFormat().format(value);
};

const formatDecimal = (value, { minimumFractionDigits = 1, maximumFractionDigits = 1 } = {}) => {
  if (value === null || value === undefined) {
    return '–';
  }
  const numeric = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(numeric)) {
    return '–';
  }
  return new Intl.NumberFormat(undefined, {
    minimumFractionDigits,
    maximumFractionDigits,
  }).format(numeric);
};

const formatPowerKw = (value) => {
  if (value === null || value === undefined) {
    return null;
  }
  let numeric;
  if (typeof value === 'number') {
    numeric = value;
  } else if (typeof value === 'string') {
    const normalized = value.replace(',', '.');
    numeric = Number.parseFloat(normalized);
  } else {
    return null;
  }
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return null;
  }
  const fractionDigits = numeric < 10 ? 1 : 0;
  return `${new Intl.NumberFormat(undefined, {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits,
  }).format(numeric)} kW`;
};

const formatChargerType = (value) => {
  if (!value) {
    return null;
  }
  const normalized = String(value).trim().toLowerCase();
  switch (normalized) {
    case 'car':
      return 'Car chargers';
    case 'motorcycle':
      return 'Motorcycle chargers';
    case 'both':
      return 'Car & motorcycle chargers';
    default:
      return null;
  }
};

const formatPercent = (value, fractionDigits = 1) => {
  if (value === null || value === undefined) {
    return '–';
  }
  const numeric = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(numeric)) {
    return '–';
  }
  return `${formatDecimal(numeric, {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits,
  })}%`;
};

const formatRatioPercent = (value, fractionDigits = 1) => {
  if (value === null || value === undefined) {
    return '–';
  }
  const numeric = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(numeric)) {
    return '–';
  }
  return formatPercent(numeric * 100, fractionDigits);
};

const formatDistance = (meters) => {
  if (!Number.isFinite(meters)) {
    return '–';
  }
  if (meters >= 1000) {
    return `${formatDecimal(meters / 1000, {
      minimumFractionDigits: 1,
      maximumFractionDigits: 1,
    })} km`;
  }
  if (meters >= 1) {
    return `${Math.round(meters)} m`;
  }
  return '<1 m';
};

const formatMinutes = (value) => {
  if (!value && value !== 0) {
    return '–';
  }
  return `${value.toFixed(1)} min`;
};

const formatDurationMinutes = (value) => {
  if (value === null || value === undefined) {
    return '–';
  }
  const numeric = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(numeric)) {
    return '–';
  }
  if (numeric <= 0) {
    return '0 min';
  }
  if (numeric < 1) {
    return '<1 min';
  }
  const totalMinutes = Math.round(numeric);
  const days = Math.floor(totalMinutes / 1440);
  const hours = Math.floor((totalMinutes % 1440) / 60);
  const minutes = totalMinutes % 60;
  const parts = [];
  if (days > 0) {
    parts.push(`${days}d`);
  }
  if (hours > 0) {
    parts.push(`${hours}h`);
  }
  if (minutes > 0 && parts.length < 2) {
    parts.push(`${minutes}m`);
  }
  if (parts.length === 0) {
    parts.push(`${totalMinutes} min`);
  }
  return parts.join(' ');
};

const formatDate = (isoDate, { includeWeekday = true } = {}) => {
  if (!isoDate) {
    return '–';
  }
  const date = new Date(isoDate);
  if (Number.isNaN(date)) {
    return isoDate;
  }
  const options = {
    month: 'short',
    day: 'numeric',
    ...(includeWeekday ? { weekday: 'short' } : {}),
  };
  const now = new Date();
  if (date.getFullYear() !== now.getFullYear()) {
    options.year = 'numeric';
  }
  return date.toLocaleDateString(undefined, options);
};

const formatDateTime = (
  isoDate,
  { includeWeekday = true, includeTime = true } = {}
) => {
  if (!isoDate) {
    return '–';
  }
  const date = new Date(isoDate);
  if (Number.isNaN(date)) {
    return isoDate;
  }
  const options = {
    month: 'short',
    day: 'numeric',
    ...(includeWeekday ? { weekday: 'short' } : {}),
  };
  if (includeTime) {
    options.hour = '2-digit';
    options.minute = '2-digit';
  }
  const now = new Date();
  if (date.getFullYear() !== now.getFullYear()) {
    options.year = 'numeric';
  }
  return date.toLocaleString(undefined, options);
};

const formatRange = (startIso, endIso) => {
  if (!startIso || !endIso) {
    return null;
  }
  const start = new Date(startIso);
  const end = new Date(endIso);
  if (Number.isNaN(start) || Number.isNaN(end)) {
    return null;
  }
  const now = new Date();
  const startOptions = { month: 'short', day: 'numeric' };
  const endOptions = { month: 'short', day: 'numeric' };
  if (start.getFullYear() !== now.getFullYear() || start.getFullYear() !== end.getFullYear()) {
    startOptions.year = 'numeric';
  }
  if (end.getFullYear() !== now.getFullYear() || start.getFullYear() !== end.getFullYear()) {
    endOptions.year = 'numeric';
  }
  return `${start.toLocaleDateString(undefined, startOptions)} – ${end.toLocaleDateString(undefined, endOptions)}`;
};

const setActiveUtilizationTab = (targetId) => {
  if (!utilizationTabsRoot || utilizationTabButtons.length === 0) {
    return;
  }
  const targetButton = utilizationTabButtons.find((button) => button.dataset.tab === targetId);
  if (!targetButton) {
    return;
  }
  activeUtilizationTabId = targetId;
  utilizationTabButtons.forEach((button) => {
    const isActive = button === targetButton;
    button.classList.toggle('is-active', isActive);
    button.setAttribute('aria-selected', isActive ? 'true' : 'false');
    button.tabIndex = isActive ? 0 : -1;
  });
  utilizationPanels.forEach((panel) => {
    const isActive = panel.dataset.utilizationPanel === targetId;
    panel.hidden = !isActive;
    panel.setAttribute('aria-hidden', isActive ? 'false' : 'true');
    panel.classList.toggle('is-active', isActive);
  });
};

const updateUtilizationSortIndicators = () => {
  if (utilizationRateHeader) {
    utilizationRateHeader.setAttribute('aria-sort', utilizationSortDescending ? 'descending' : 'ascending');
  }
  if (utilizationSortButton) {
    const indicator = utilizationSortButton.querySelector('.sort-indicator');
    if (indicator) {
      indicator.textContent = utilizationSortDescending ? 'High → Low' : 'Low → High';
    }
    const nextOrder = utilizationSortDescending ? 'low to high' : 'high to low';
    utilizationSortButton.setAttribute('aria-label', `Sort utilization rate ${nextOrder}`);
    utilizationSortButton.setAttribute('title', `Sort utilization rate ${nextOrder}`);
  }
};

const getLocationUtilizationValue = (row) => {
  if (!row) {
    return Number.NaN;
  }
  const value = Number(row.occupation_utilization_pct);
  return Number.isFinite(value) ? value : Number.NaN;
};

const renderTableMessage = (body, message, colSpan = 4) => {
  if (!body) {
    return;
  }
  const tr = document.createElement('tr');
  const td = document.createElement('td');
  td.colSpan = colSpan;
  td.textContent = message;
  td.classList.add('muted');
  tr.appendChild(td);
  body.appendChild(tr);
};

const renderUtilizationLocationRows = () => {
  if (!utilizationLocationsBody) {
    return;
  }
  utilizationLocationsBody.innerHTML = '';

  if (utilizationLocationRows === null) {
    renderTableMessage(utilizationLocationsBody, 'Loading location utilization…');
    clearUtilizationFooter('locations');
    return;
  }

  if (!Array.isArray(utilizationLocationRows) || utilizationLocationRows.length === 0) {
    renderTableMessage(utilizationLocationsBody, 'Location utilization data unavailable.');
    clearUtilizationFooter('locations');
    return;
  }

  const sortedRows = [...utilizationLocationRows];
  const descending = utilizationSortDescending;
  const normalize = (value) => {
    if (Number.isFinite(value)) {
      return value;
    }
    return descending ? -Infinity : Infinity;
  };
  sortedRows.sort((a, b) => {
    const aValue = normalize(getLocationUtilizationValue(a));
    const bValue = normalize(getLocationUtilizationValue(b));
    return descending ? bValue - aValue : aValue - bValue;
  });

  const totalRows = sortedRows.length;
  const visibleCount = ensureVisibleRowCount('locations', totalRows);
  const rowsToRender = sortedRows.slice(0, visibleCount);

  rowsToRender.forEach((row) => {
    const tr = document.createElement('tr');

    const hasLocationId =
      row && row.location_id !== null && row.location_id !== undefined && row.location_id !== '';
    const locationCell = document.createElement('td');
    if (hasLocationId) {
      const link = document.createElement('a');
      link.href = `location.html?id=${encodeURIComponent(String(row.location_id))}`;
      link.className = 'table-link';
      link.textContent = String(row.location_id);
      link.title = `View detailed insights for location ${row.location_id}`;
      locationCell.appendChild(link);
    } else {
      locationCell.textContent = 'Unknown';
      locationCell.classList.add('muted');
    }
    tr.appendChild(locationCell);

    const stationCell = document.createElement('td');
    stationCell.textContent = formatNumber(row?.station_count);
    tr.appendChild(stationCell);

    const portCell = document.createElement('td');
    portCell.textContent = formatNumber(row?.port_count);
    tr.appendChild(portCell);

    const utilizationCell = document.createElement('td');
    const utilizationValue = getLocationUtilizationValue(row);
    utilizationCell.textContent = Number.isFinite(utilizationValue)
      ? formatPercent(utilizationValue, 1)
      : '–';
    tr.appendChild(utilizationCell);

    utilizationLocationsBody.appendChild(tr);
  });

  updateUtilizationFooter('locations', totalRows, rowsToRender.length);
};

const renderUtilizationStationRows = () => {
  if (!utilizationStationsBody) {
    return;
  }
  utilizationStationsBody.innerHTML = '';

  if (utilizationStationRows === null) {
    renderTableMessage(utilizationStationsBody, 'Loading station utilization…');
    clearUtilizationFooter('stations');
    return;
  }

  if (!Array.isArray(utilizationStationRows) || utilizationStationRows.length === 0) {
    renderTableMessage(utilizationStationsBody, 'Station utilization data unavailable.');
    clearUtilizationFooter('stations');
    return;
  }

  const totalRows = utilizationStationRows.length;
  const visibleCount = ensureVisibleRowCount('stations', totalRows);
  const rowsToRender = utilizationStationRows.slice(0, visibleCount);

  rowsToRender.forEach((row) => {
    const tr = document.createElement('tr');

    const locationCell = document.createElement('td');
    if (row?.location_id) {
      const link = document.createElement('a');
      link.href = `location.html?id=${encodeURIComponent(String(row.location_id))}`;
      link.className = 'table-link';
      link.textContent = String(row.location_id);
      link.title = `View detailed insights for location ${row.location_id}`;
      locationCell.appendChild(link);
    } else {
      locationCell.textContent = 'Unknown';
      locationCell.classList.add('muted');
    }
    tr.appendChild(locationCell);

    const stationCell = document.createElement('td');
    if (row?.station_id) {
      stationCell.textContent = String(row.station_id);
    } else {
      stationCell.textContent = 'Unknown';
      stationCell.classList.add('muted');
    }
    tr.appendChild(stationCell);

    const portCell = document.createElement('td');
    portCell.textContent = formatNumber(row?.port_count);
    tr.appendChild(portCell);

    const utilizationCell = document.createElement('td');
    const utilizationValue = Number(row?.occupation_utilization_pct);
    utilizationCell.textContent = Number.isFinite(utilizationValue)
      ? formatPercent(utilizationValue, 1)
      : '–';
    tr.appendChild(utilizationCell);

    utilizationStationsBody.appendChild(tr);
  });

  updateUtilizationFooter('stations', totalRows, rowsToRender.length);
};

const renderUtilizationPortRows = () => {
  if (!utilizationPortsBody) {
    return;
  }
  utilizationPortsBody.innerHTML = '';

  if (utilizationPortRows === null) {
    renderTableMessage(utilizationPortsBody, 'Loading port utilization…');
    clearUtilizationFooter('ports');
    return;
  }

  if (!Array.isArray(utilizationPortRows) || utilizationPortRows.length === 0) {
    renderTableMessage(utilizationPortsBody, 'Port utilization data unavailable.');
    clearUtilizationFooter('ports');
    return;
  }

  const totalRows = utilizationPortRows.length;
  const visibleCount = ensureVisibleRowCount('ports', totalRows);
  const rowsToRender = utilizationPortRows.slice(0, visibleCount);

  rowsToRender.forEach((row) => {
    const tr = document.createElement('tr');

    const locationCell = document.createElement('td');
    if (row?.location_id) {
      const link = document.createElement('a');
      link.href = `location.html?id=${encodeURIComponent(String(row.location_id))}`;
      link.className = 'table-link';
      link.textContent = String(row.location_id);
      link.title = `View detailed insights for location ${row.location_id}`;
      locationCell.appendChild(link);
    } else {
      locationCell.textContent = 'Unknown';
      locationCell.classList.add('muted');
    }
    tr.appendChild(locationCell);

    const stationCell = document.createElement('td');
    if (row?.station_id) {
      stationCell.textContent = String(row.station_id);
    } else {
      stationCell.textContent = 'Unknown';
      stationCell.classList.add('muted');
    }
    tr.appendChild(stationCell);

    const portCell = document.createElement('td');
    if (row?.port_id) {
      portCell.textContent = String(row.port_id);
    } else {
      portCell.textContent = 'Unknown';
      portCell.classList.add('muted');
    }
    tr.appendChild(portCell);

    const utilizationCell = document.createElement('td');
    const utilizationValue = Number(row?.occupation_utilization_pct);
    utilizationCell.textContent = Number.isFinite(utilizationValue)
      ? formatPercent(utilizationValue, 1)
      : '–';
    tr.appendChild(utilizationCell);

    utilizationPortsBody.appendChild(tr);
  });

  updateUtilizationFooter('ports', totalRows, rowsToRender.length);
};

function refreshUtilizationTables() {
  updateUtilizationSortIndicators();
  renderUtilizationLocationRows();
  renderUtilizationStationRows();
  renderUtilizationPortRows();
}

const handleUtilizationLoadMore = (view) => {
  const state = getUtilizationViewState(view);
  if (!state) {
    return;
  }
  const totalRows = getUtilizationRowCount(view);
  if (totalRows <= 0) {
    state.visible = 0;
    return;
  }
  const limit = Math.max(1, Number(state.limit) || UTILIZATION_DEFAULT_PAGE_SIZE);
  const currentVisible =
    Number.isFinite(state.visible) && state.visible > 0 ? state.visible : limit;
  state.visible = Math.min(totalRows, currentVisible + limit);
  refreshUtilizationTables();
};

const setUtilizationData = (locations, stations, ports) => {
  if (Array.isArray(locations)) {
    utilizationLocationRows = locations.slice();
    utilizationSortDescending = true;
    resetUtilizationView('locations');
  } else if (locations === null) {
    utilizationLocationRows = null;
    resetUtilizationView('locations');
  } else {
    utilizationLocationRows = [];
    utilizationSortDescending = true;
    resetUtilizationView('locations');
  }

  if (Array.isArray(stations)) {
    utilizationStationRows = stations.slice();
    resetUtilizationView('stations');
  } else if (stations === null) {
    utilizationStationRows = null;
    resetUtilizationView('stations');
  } else {
    utilizationStationRows = [];
    resetUtilizationView('stations');
  }

  if (Array.isArray(ports)) {
    utilizationPortRows = ports.slice();
    resetUtilizationView('ports');
  } else if (ports === null) {
    utilizationPortRows = null;
    resetUtilizationView('ports');
  } else {
    utilizationPortRows = [];
    resetUtilizationView('ports');
  }

  refreshUtilizationTables();
};

const resolveHeatmapMetric = (metric) => {
  if (metric && Object.prototype.hasOwnProperty.call(HEATMAP_METRICS, metric)) {
    return metric;
  }
  return HEATMAP_DEFAULT_METRIC;
};

const setHeatmapStatus = (message) => {
  if (!heatmapStatusEl) {
    return;
  }
  if (message) {
    heatmapStatusEl.textContent = message;
    heatmapStatusEl.hidden = false;
  } else {
    heatmapStatusEl.hidden = true;
  }
};

const setHeatmapListPlaceholder = (message, summary = '') => {
  if (!heatmapListContainer) {
    return;
  }
  heatmapListContainer.innerHTML = '';
  if (message) {
    const note = document.createElement('p');
    note.className = 'muted';
    note.textContent = message;
    heatmapListContainer.appendChild(note);
  }
  if (heatmapUpdatedEl) {
    heatmapUpdatedEl.textContent = summary;
  }
};

const updateHeatmapDescription = () => {
  if (!heatmapDescriptionEl) {
    return;
  }
  const definition = HEATMAP_METRICS[resolveHeatmapMetric(heatmapMetricKey)];
  const description = definition?.description ?? '';
  heatmapDescriptionEl.textContent = description;
  heatmapDescriptionEl.hidden = !description;
};

const isMapLibreAvailable = () =>
  typeof window !== 'undefined' && window.maplibregl && typeof window.maplibregl.Map === 'function';

const ensureHeatmapMap = () => {
  if (!heatmapMapContainer) {
    return null;
  }
  if (heatmapMap) {
    return heatmapMap;
  }
  if (!isMapLibreAvailable()) {
    setHeatmapStatus('Map library failed to load.');
    if (heatmapNoteEl) {
      heatmapNoteEl.textContent = 'Map unavailable because MapLibre GL JS failed to load.';
    }
    return null;
  }

  const maplibregl = window.maplibregl;
  heatmapMap = new maplibregl.Map({
    container: heatmapMapContainer,
    style: MAP_STYLE_URL,
    center: MAP_DEFAULT_CENTER,
    zoom: MAP_DEFAULT_ZOOM,
    attributionControl: false,
    cooperativeGestures: true,
  });

  heatmapMapReady = false;

  const attribution = new maplibregl.AttributionControl({
    compact: true,
    customAttribution: MAP_ATTRIBUTION,
  });
  heatmapMap.addControl(attribution, 'bottom-right');

  if (typeof maplibregl.NavigationControl === 'function') {
    heatmapMap.addControl(new maplibregl.NavigationControl({ visualizePitch: false }), 'top-right');
  }

  const handleResize = () => {
    if (heatmapMap) {
      heatmapMap.resize();
    }
  };

  if (typeof window.addEventListener === 'function') {
    heatmapResizeHandle = handleResize;
    window.addEventListener('resize', heatmapResizeHandle);
  }

  if (typeof ResizeObserver === 'function') {
    heatmapResizeObserver = new ResizeObserver(handleResize);
    heatmapResizeObserver.observe(heatmapMapContainer);
  }

  heatmapMap.on('load', () => {
    heatmapMapReady = true;

    if (!heatmapMap?.getSource(HEATMAP_SOURCE_ID)) {
      heatmapMap.addSource(HEATMAP_SOURCE_ID, {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] },
      });
    }

    if (!heatmapMap.getLayer(HEATMAP_HEAT_LAYER_ID)) {
      heatmapMap.addLayer({
        id: HEATMAP_HEAT_LAYER_ID,
        type: 'heatmap',
        source: HEATMAP_SOURCE_ID,
        paint: {
          'heatmap-radius': [
            'interpolate',
            ['linear'],
            ['zoom'],
            10,
            20,
            13,
            32,
            16,
            48,
          ],
          'heatmap-intensity': ['interpolate', ['linear'], ['zoom'], 10, 1.1, 16, 2.6],
          'heatmap-opacity': 0.75,
          'heatmap-weight': ['coalesce', ['get', 'intensity'], 0],
        },
      });
    }

    if (!heatmapMap.getLayer(HEATMAP_CIRCLE_LAYER_ID)) {
      heatmapMap.addLayer({
        id: HEATMAP_CIRCLE_LAYER_ID,
        type: 'circle',
        source: HEATMAP_SOURCE_ID,
        minzoom: 11,
        paint: {
          'circle-radius': [
            'interpolate',
            ['linear'],
            ['get', 'intensity'],
            0,
            6,
            1,
            14,
          ],
          'circle-color': [
            'interpolate',
            ['linear'],
            ['get', 'intensity'],
            0,
            'hsl(210, 85%, 55%)',
            0.5,
            'hsl(120, 85%, 50%)',
            1,
            'hsl(0, 85%, 45%)',
          ],
          'circle-stroke-color': 'rgba(15, 23, 42, 0.55)',
          'circle-stroke-width': 1,
          'circle-opacity': 0.9,
        },
      });
    }

    if (!heatmapPopup) {
      heatmapPopup = new maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 12 });
    }

    heatmapMap.on('mouseenter', HEATMAP_CIRCLE_LAYER_ID, () => {
      if (heatmapMap) {
        heatmapMap.getCanvas().style.cursor = 'pointer';
      }
    });

    heatmapMap.on('mouseleave', HEATMAP_CIRCLE_LAYER_ID, () => {
      if (heatmapMap) {
        heatmapMap.getCanvas().style.cursor = '';
      }
      if (heatmapPopup) {
        heatmapPopup.remove();
      }
    });

    heatmapMap.on('mousemove', HEATMAP_CIRCLE_LAYER_ID, (event) => {
      const feature = event?.features?.[0];
      if (!feature || !heatmapPopup) {
        return;
      }
      const [lon, lat] = feature.geometry?.coordinates || [];
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
        return;
      }
      const props = feature.properties || {};
      const stationCount = Number.parseInt(props.stationCount, 10);
      const portCount = Number.parseInt(props.portCount, 10);
      const value = Number.parseFloat(props.value);
      const container = document.createElement('div');
      const title = document.createElement('strong');
      title.textContent = `Location ${props.id ?? ''}`.trim();
      container.appendChild(title);
      const metricLine = document.createElement('div');
      const definition = HEATMAP_METRICS[resolveHeatmapMetric(heatmapMetricKey)];
      const label = definition?.label?.toLowerCase() || 'utilization';
      metricLine.textContent = `${formatPercent(value, 1)} ${label}`;
      container.appendChild(metricLine);
      if (Number.isFinite(stationCount) || Number.isFinite(portCount)) {
        const counts = document.createElement('div');
        const stationText = Number.isFinite(stationCount)
          ? `${stationCount} station${stationCount === 1 ? '' : 's'}`
          : null;
        const portText = Number.isFinite(portCount)
          ? `${portCount} port${portCount === 1 ? '' : 's'}`
          : null;
        counts.textContent = [stationText, portText].filter(Boolean).join(' · ');
        container.appendChild(counts);
      }
      if (props.address) {
        const address = document.createElement('div');
        address.textContent = props.address;
        container.appendChild(address);
      }
      heatmapPopup.setLngLat([lon, lat]).setDOMContent(container).addTo(heatmapMap);
    });

    heatmapMap.on('click', HEATMAP_CIRCLE_LAYER_ID, (event) => {
      const feature = event?.features?.[0];
      const id = feature?.properties?.id;
      if (!id) {
        return;
      }
      const targetUrl = `location.html?id=${encodeURIComponent(id)}`;
      const originalEvent = event?.originalEvent;
      if (originalEvent && (originalEvent.ctrlKey || originalEvent.metaKey || originalEvent.button === 1)) {
        window.open(targetUrl, '_blank', 'noopener');
      } else {
        window.location.href = targetUrl;
      }
    });

    applyHeatmapEntries(heatmapLatestEntries);
    if (Array.isArray(heatmapLatestEntries) && heatmapLatestEntries.length > 0) {
      setHeatmapStatus(null);
    }
    if (heatmapPendingBounds) {
      heatmapMap.fitBounds(heatmapPendingBounds, {
        padding: 60,
        maxZoom: HEATMAP_MAX_POINT_ZOOM,
        duration: 0,
      });
      heatmapPendingBounds = undefined;
    }
  });

  if (heatmapNoteEl) {
    heatmapNoteEl.textContent = 'Map data © OpenStreetMap contributors, © CARTO.';
  }

  return heatmapMap;
};

const toHeatmapFeatureCollection = (entries) => ({
  type: 'FeatureCollection',
  features: entries.map((entry) => ({
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [entry.lon, entry.lat] },
    properties: {
      id: entry.id,
      value: entry.value,
      intensity: entry.intensity,
      stationCount: entry.stationCount ?? '',
      portCount: entry.portCount ?? '',
      address: entry.address ?? '',
    },
  })),
});

const updateHeatmapSourceData = (entries) => {
  if (!heatmapMapReady) {
    return;
  }
  const source = heatmapMap?.getSource(HEATMAP_SOURCE_ID);
  if (source) {
    source.setData(toHeatmapFeatureCollection(entries));
  }
};

const updateHeatmapBounds = (entries) => {
  if (!entries.length) {
    return;
  }
  if (!heatmapMapReady || !heatmapMap) {
    if (!isMapLibreAvailable()) {
      heatmapPendingBounds = undefined;
      return;
    }
    const bounds = entries.reduce((acc, entry) => {
      if (!acc) {
        return [[entry.lon, entry.lat], [entry.lon, entry.lat]];
      }
      const [[west, south], [east, north]] = acc;
      return [
        [Math.min(west, entry.lon), Math.min(south, entry.lat)],
        [Math.max(east, entry.lon), Math.max(north, entry.lat)],
      ];
    }, null);
    heatmapPendingBounds = bounds
      ? new window.maplibregl.LngLatBounds(bounds[0], bounds[1])
      : undefined;
    return;
  }

  if (typeof window.maplibregl?.LngLatBounds !== 'function') {
    return;
  }

  const first = entries[0];
  const bounds = entries.slice(1).reduce(
    (acc, entry) => acc.extend([entry.lon, entry.lat]),
    new window.maplibregl.LngLatBounds([first.lon, first.lat], [first.lon, first.lat]),
  );

  if (!heatmapMap) {
    return;
  }

  if (entries.length === 1) {
    heatmapMap.jumpTo({ center: [first.lon, first.lat], zoom: HEATMAP_MAX_POINT_ZOOM });
    return;
  }

  if (bounds) {
    heatmapMap.fitBounds(bounds, { padding: 60, maxZoom: HEATMAP_MAX_POINT_ZOOM, duration: 0 });
  }
};

const applyHeatmapEntries = (entries) => {
  heatmapLatestEntries = entries.slice();
  updateHeatmapSourceData(entries);
  if (heatmapPopup) {
    heatmapPopup.remove();
  }
  updateHeatmapBounds(entries);
};

const clearHeatmapLayers = () => {
  heatmapLatestEntries = [];
  updateHeatmapSourceData([]);
  if (heatmapPopup) {
    heatmapPopup.remove();
  }
  heatmapPendingBounds = undefined;
};

function heatmapColorForValue(value) {
  if (!Array.isArray(HEATMAP_COLOR_STOPS) || HEATMAP_COLOR_STOPS.length === 0) {
    return '';
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return '';
  }
  const clamped = Math.min(Math.max(numeric, 0), 100);
  const intensity = clamped / 100;
  for (let index = 0; index < HEATMAP_COLOR_STOPS.length - 1; index += 1) {
    const current = HEATMAP_COLOR_STOPS[index];
    const next = HEATMAP_COLOR_STOPS[index + 1];
    if (!current || !next) {
      continue;
    }
    if (intensity <= next.stop) {
      const span = next.stop - current.stop;
      const progress = span === 0 ? 0 : (intensity - current.stop) / span;
      const hue = current.h + (next.h - current.h) * progress;
      const saturation = current.s + (next.s - current.s) * progress;
      const lightness = current.l + (next.l - current.l) * progress;
      return `hsl(${hue.toFixed(1)}deg, ${saturation.toFixed(1)}%, ${lightness.toFixed(1)}%)`;
    }
  }
  const last = HEATMAP_COLOR_STOPS[HEATMAP_COLOR_STOPS.length - 1];
  if (!last) {
    return '';
  }
  return `hsl(${last.h.toFixed(1)}deg, ${last.s.toFixed(1)}%, ${last.l.toFixed(1)}%)`;
}

function createHeatmapTableLegend(metricLabel) {
  const resolvedLabel = typeof metricLabel === 'string' && metricLabel.trim()
    ? metricLabel.trim()
    : 'Utilization';
  const legend = document.createElement('div');
  legend.className = 'heatmap-table-legend';
  legend.setAttribute('role', 'group');
  legend.setAttribute('aria-label', `${resolvedLabel} colour key`);
  const lower = document.createElement('span');
  lower.className = 'heatmap-table-legend__text';
  lower.textContent = `Lower ${resolvedLabel.toLowerCase()}`;
  const gradient = document.createElement('div');
  gradient.className = 'heatmap-table-legend-gradient';
  gradient.setAttribute('aria-hidden', 'true');
  const higher = document.createElement('span');
  higher.className = 'heatmap-table-legend__text';
  higher.textContent = `Higher ${resolvedLabel.toLowerCase()}`;
  legend.append(lower, gradient, higher);
  return legend;
}

const renderHeatmapList = (entries, metricKey) => {
  if (!heatmapListContainer) {
    return;
  }
  heatmapListContainer.innerHTML = '';
  const definition = HEATMAP_METRICS[resolveHeatmapMetric(metricKey)];
  const label = definition?.label ?? 'Utilization';
  const sorted = entries.slice().sort((a, b) => b.value - a.value);
  const limit = Math.min(sorted.length, 15);

  if (limit > 0) {
    heatmapListContainer.appendChild(createHeatmapTableLegend(label));
  }

  const wrapper = document.createElement('div');
  wrapper.className = 'table-scroll';
  const table = document.createElement('table');
  table.className = 'heatmap-table';

  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');
  ['Location', 'Stations', 'Ports', label].forEach((heading) => {
    const th = document.createElement('th');
    th.scope = 'col';
    th.textContent = heading;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  table.appendChild(thead);

  const tbody = document.createElement('tbody');
  sorted.slice(0, limit).forEach((entry) => {
    const tr = document.createElement('tr');

    const locationCell = document.createElement('td');
    locationCell.className = 'heatmap-location-cell';
    const link = document.createElement('a');
    link.href = `location.html?id=${encodeURIComponent(entry.id)}`;
    link.textContent = entry.id;
    link.setAttribute('aria-label', `View utilization details for location ${entry.id}`);
    locationCell.appendChild(link);
    if (entry.address) {
      const address = document.createElement('span');
      address.className = 'heatmap-address';
      address.textContent = entry.address;
      locationCell.appendChild(address);
    }
    tr.appendChild(locationCell);

    const stationCell = document.createElement('td');
    stationCell.textContent = Number.isFinite(entry.stationCount)
      ? formatNumber(entry.stationCount)
      : '–';
    tr.appendChild(stationCell);

    const portCell = document.createElement('td');
    portCell.textContent = Number.isFinite(entry.portCount)
      ? formatNumber(entry.portCount)
      : '–';
    tr.appendChild(portCell);

    const metricCell = document.createElement('td');
    metricCell.className = 'heatmap-metric-cell';
    const metricSwatch = document.createElement('span');
    metricSwatch.className = 'heatmap-metric-cell__swatch';
    metricSwatch.setAttribute('aria-hidden', 'true');
    const metricColour = heatmapColorForValue(entry.value);
    if (metricColour) {
      metricSwatch.style.setProperty('--heatmap-metric-color', metricColour);
    }
    const metricValue = document.createElement('span');
    metricValue.className = 'heatmap-metric-cell__value';
    metricValue.textContent = formatPercent(entry.value, 1);
    metricCell.append(metricSwatch, metricValue);
    tr.appendChild(metricCell);

    tbody.appendChild(tr);
  });

  table.appendChild(tbody);
  wrapper.appendChild(table);
  heatmapListContainer.appendChild(wrapper);

  if (heatmapUpdatedEl) {
    const summaryLabel = label.toLowerCase();
    heatmapUpdatedEl.textContent = `Top ${limit} of ${entries.length} mapped locations ranked by ${summaryLabel}.`;
  }
};

const updateHeatmapLegend = (minValue, maxValue) => {
  if (heatmapLegendMinEl) {
    heatmapLegendMinEl.textContent = Number.isFinite(minValue)
      ? formatPercent(minValue, 0)
      : '0%';
  }
  if (heatmapLegendMaxEl) {
    heatmapLegendMaxEl.textContent = Number.isFinite(maxValue)
      ? formatPercent(maxValue, 0)
      : '100%';
  }
};

const setHeatmapMetric = (metric) => {
  const resolved = resolveHeatmapMetric(metric);
  heatmapMetricKey = resolved;
  if (heatmapMetricSelect && heatmapMetricSelect.value !== resolved) {
    heatmapMetricSelect.value = resolved;
  }
  updateHeatmapDescription();
  renderHeatmap();
};

const renderHeatmap = () => {
  if (!heatmapMapContainer && !heatmapListContainer) {
    return;
  }

  if (heatmapDataRows === null) {
    setHeatmapStatus('Loading utilization heatmap…');
    setHeatmapListPlaceholder('Loading utilization hotspots…', 'Loading utilization hotspots…');
    clearHeatmapLayers();
    return;
  }

  if (!Array.isArray(heatmapDataRows) || heatmapDataRows.length === 0) {
    setHeatmapStatus('Utilization data unavailable.');
    setHeatmapListPlaceholder('Utilization data unavailable.', '');
    clearHeatmapLayers();
    return;
  }

  if (!heatmapCoordinateLookup || Object.keys(heatmapCoordinateLookup).length === 0) {
    setHeatmapStatus('Location coordinates unavailable.');
    setHeatmapListPlaceholder('Location coordinates unavailable.', '');
    clearHeatmapLayers();
    return;
  }

  const mapInstance = ensureHeatmapMap();
  if (!mapInstance) {
    setHeatmapListPlaceholder('Map unavailable.', '');
    return;
  }

  const metricKey = resolveHeatmapMetric(heatmapMetricKey);
  const definition = HEATMAP_METRICS[metricKey];
  updateHeatmapDescription();

  const entries = heatmapDataRows
    .map((row) => {
      const id = row?.location_id || row?.locationId || row?.id;
      if (!id) {
        return null;
      }
      const coords = heatmapCoordinateLookup?.[id];
      if (!coords) {
        return null;
      }
      const lat = Number(coords.lat);
      const lon = Number(coords.lon);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
        return null;
      }
      const metricValueRaw = row?.[metricKey];
      const metricValue = Number(
        typeof metricValueRaw === 'number' ? metricValueRaw : Number(metricValueRaw)
      );
      if (!Number.isFinite(metricValue)) {
        return null;
      }
      const stationCountRaw = row?.station_count ?? row?.stationCount;
      const portCountRaw = row?.port_count ?? row?.portCount;
      const stationCount = Number(stationCountRaw);
      const portCount = Number(portCountRaw);
      const addressValue = coords?.address;
      const address = typeof addressValue === 'string' && addressValue.trim()
        ? addressValue.trim()
        : null;
      return {
        id,
        lat,
        lon,
        value: metricValue,
        intensity: Math.min(Math.max(metricValue / 100, 0), 1),
        stationCount: Number.isFinite(stationCount) ? stationCount : null,
        portCount: Number.isFinite(portCount) ? portCount : null,
        address,
      };
    })
    .filter(Boolean);

  if (entries.length === 0) {
    setHeatmapStatus('No locations have utilization data with coordinates.');
    setHeatmapListPlaceholder('No mappable locations available.', '');
    clearHeatmapLayers();
    return;
  }

  applyHeatmapEntries(entries);
  if (heatmapMapReady) {
    setHeatmapStatus(null);
  } else {
    setHeatmapStatus('Preparing map…');
  }

  const values = entries.map((entry) => entry.value);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  updateHeatmapLegend(minValue, maxValue);

  renderHeatmapList(entries, metricKey);
};

const setHeatmapData = (locations, coordinates) => {
  if (!heatmapMapContainer && !heatmapListContainer) {
    return;
  }
  heatmapCoordinateLookup =
    coordinates && typeof coordinates === 'object' ? coordinates : null;
  if (locations === null) {
    heatmapDataRows = null;
  } else if (Array.isArray(locations)) {
    heatmapDataRows = locations.slice();
  } else {
    heatmapDataRows = [];
  }
  renderHeatmap();
};

document.querySelectorAll('[data-utilization-footer]').forEach((element) => {
  const view = element?.dataset?.utilizationFooter;
  if (!view || !UTILIZATION_VIEW_IDS.includes(view)) {
    return;
  }
  utilizationFooterContainers.set(view, element);
  element.hidden = true;
  element.addEventListener('click', (event) => {
    const button = event.target.closest('[data-utilization-more]');
    if (!button || !element.contains(button)) {
      return;
    }
    const targetView = button.dataset.utilizationMore;
    if (targetView === view) {
      handleUtilizationLoadMore(view);
    }
  });
});

document.querySelectorAll('[data-utilization-limit]').forEach((element) => {
  if (!(element instanceof HTMLSelectElement)) {
    return;
  }
  const view = element.dataset.utilizationLimit;
  if (!view || !UTILIZATION_VIEW_IDS.includes(view)) {
    return;
  }
  const state = getUtilizationViewState(view);
  const initial = parseUtilizationPageSize(element.value || state?.limit);
  if (state) {
    state.limit = initial;
  }
  element.value = String(initial);
  utilizationLimitControls.set(view, element);
  element.addEventListener('change', (event) => {
    const target = event.target;
    if (!(target instanceof HTMLSelectElement)) {
      return;
    }
    const next = parseUtilizationPageSize(target.value);
    const viewState = getUtilizationViewState(view);
    if (viewState) {
      viewState.limit = next;
      resetUtilizationView(view);
    }
    refreshUtilizationTables();
  });
});

if (heatmapMetricSelect) {
  const initialMetric = resolveHeatmapMetric(heatmapMetricSelect.value || heatmapMetricKey);
  heatmapMetricKey = initialMetric;
  heatmapMetricSelect.value = initialMetric;
  heatmapMetricSelect.addEventListener('change', (event) => {
    const target = event.target;
    if (!(target instanceof HTMLSelectElement)) {
      return;
    }
    setHeatmapMetric(target.value);
  });
  updateHeatmapDescription();
}

const formatPeriodRange = (startIso, endIso, granularity) => {
  if (!startIso) {
    return null;
  }
  if (granularity === 'hour') {
    const start = formatDateTime(startIso);
    if (!start || start === '–') {
      return null;
    }
    if (!endIso) {
      return start;
    }
    const end = formatDateTime(endIso);
    if (!end || end === '–') {
      return start;
    }
    return `${start} – ${end}`;
  }
  return formatRange(startIso, endIso);
};

const determineGranularity = (days) => {
  if (days <= 7) {
    return 'hour';
  }
  return 'day';
};

const formatTooltipTitle = (chart, dataIndex) => {
  const timelineForChart = chart.$timeline || [];
  const entry = timelineForChart[dataIndex];
  if (!entry) {
    return '';
  }
  const granularityForChart = chart.$granularity || 'day';
  if (granularityForChart === 'hour') {
    return formatDateTime(entry.start);
  }
  return formatDate(entry.day || entry.start);
};

const getRangeOptions = () => {
  if (!chargesRangeControl) {
    return [];
  }
  return Array.from(chargesRangeControl.querySelectorAll('[data-value]'));
};

const setRangeSelection = (value) => {
  if (!chargesRangeControl) {
    return;
  }
  const normalizedValue = String(clampDays(value));
  const options = getRangeOptions();
  if (options.length === 0) {
    chargesRangeControl.dataset.selected = normalizedValue;
    return;
  }
  const activeOption =
    options.find((option) => option.dataset.value === normalizedValue) ?? options[0];
  const targetValue = activeOption.dataset.value;
  chargesRangeControl.dataset.selected = targetValue;
  options.forEach((option) => {
    const isActive = option === activeOption;
    option.classList.toggle('is-active', isActive);
    option.setAttribute('aria-checked', isActive ? 'true' : 'false');
    option.tabIndex = isActive ? 0 : -1;
  });
};

const setChartStatus = (message) => {
  if (!chargesStatus) {
    return;
  }
  if (!message) {
    chargesStatus.hidden = true;
    return;
  }
  chargesStatus.textContent = message;
  chargesStatus.hidden = false;
};

const updateSummary = (stats, info) => {
  const mapping = {
    chargers: 'summary-chargers',
    unavailable: 'summary-unavailable',
    charging: 'summary-charging',
    sessions: 'summary-sessions',
    charges_today: 'summary-charges-today',
  };
  Object.entries(mapping).forEach(([key, id]) => {
    const el = document.getElementById(id);
    if (el) {
      el.textContent = formatNumber(stats?.[key]);
    }
  });
  const avgEl = document.getElementById('summary-avg-session');
  if (avgEl) {
    avgEl.textContent = formatMinutes(stats?.avg_session_min || 0);
  }

  const mttrEl = document.getElementById('summary-mttr');
  if (mttrEl) {
    mttrEl.textContent = formatDurationMinutes(stats?.mttr_minutes);
  }

  const meta = document.getElementById('summary-meta');
  if (meta) {
    const updated = info?.updated ? new Date(info.updated).toLocaleString() : 'unknown';
    const dbSize = info?.db?.size_bytes ? `${(info.db.size_bytes / (1024 * 1024)).toFixed(1)} MB` : 'unknown';
    meta.innerHTML = `<span>Last snapshot: ${updated}</span> · <span>Database size: ${dbSize}</span>`;
  }

  const updatedFooter = document.getElementById('last-updated');
  if (updatedFooter && info?.last_fetch) {
    const fetchTime = new Date(info.last_fetch).toLocaleString();
    updatedFooter.textContent = `Last fetch ${fetchTime}`;
  }
};

const updateUtilization = (utilization) => {
  const noteEl = document.getElementById('utilization-note');
  const metrics = utilization?.network;
  if (!metrics) {
    if (noteEl) {
      noteEl.textContent = 'Utilization data unavailable.';
    }
    setUtilizationLocationRows([]);
    return;
  }

  const mapping = [
    ['utilization-ports', metrics.port_count, formatNumber],
    ['utilization-stations', metrics.station_count, formatNumber],
    ['utilization-locations', metrics.location_count, formatNumber],
    [
      'utilization-session-day',
      metrics.session_count_per_day,
      (value) => formatDecimal(value, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    ],
    [
      'utilization-session-hour',
      metrics.session_count_per_hour,
      (value) => formatDecimal(value, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    ],
    ['utilization-occupied', metrics.occupation_utilization_pct, (value) => formatPercent(value, 1)],
    ['utilization-active', metrics.active_charging_utilization_pct, (value) => formatPercent(value, 1)],
    ['utilization-availability', metrics.availability_ratio, (value) => formatRatioPercent(value, 1)],
  ];

  mapping.forEach(([id, value, formatter]) => {
    const el = document.getElementById(id);
    if (el) {
      el.textContent = formatter(value);
    }
  });

  setUtilizationData(
    Array.isArray(utilization?.locations) ? utilization.locations : [],
    Array.isArray(utilization?.stations) ? utilization.stations : [],
    Array.isArray(utilization?.ports) ? utilization.ports : [],
  );

  if (noteEl) {
    const monitoredDays = Number(metrics.monitored_days);
    const ports = Number(metrics.port_count);
    if (Number.isFinite(monitoredDays) && Number.isFinite(ports) && ports > 0) {
      const averageDays = monitoredDays / ports;
      noteEl.textContent = `Averages based on the last ${formatDecimal(averageDays, {
        minimumFractionDigits: 1,
        maximumFractionDigits: 1,
      })} days of telemetry per port across ${formatNumber(ports)} ports.`;
    } else {
      noteEl.textContent = 'Utilization derived from the latest telemetry window.';
    }
  }
};

const updateRules = (rules, counts) => {
  const el = document.getElementById('rule-summary');
  if (!el) return;
  el.innerHTML = '';
  const entries = [
    {
      label: `Unused > ${rules.unused_days} days`,
      value: counts?.unused ?? 0,
    },
    {
      label: `No long session (≥ ${rules.long_session_min} min in ${rules.long_session_days} days)`,
      value: counts?.no_long ?? 0,
    },
    {
      label: `Unavailable > ${rules.unavailable_hours} hours`,
      value: counts?.unavailable ?? 0,
    },
  ];
  entries.forEach(({ label, value }) => {
    const item = document.createElement('li');
    const name = document.createElement('span');
    name.textContent = label;
    const count = document.createElement('strong');
    count.textContent = formatNumber(value);
    item.append(name, count);
    el.appendChild(item);
  });
};

const updateDaily = (daily) => {
  const tbody = document.getElementById('daily-sessions');
  if (!tbody) return;
  tbody.innerHTML = '';
  (daily || []).forEach((row) => {
    const tr = document.createElement('tr');
    const day = document.createElement('td');
    day.textContent = formatDate(row.day);
    const sessions = document.createElement('td');
    sessions.textContent = formatNumber(row.sessions);
    tr.append(day, sessions);
    tbody.appendChild(tr);
  });
};

const updateChargesSummary = (timeline, granularity) => {
  if (chargesTotalEl) {
    const total = (timeline || []).reduce((acc, entry) => acc + (entry?.sessions ?? 0), 0);
    chargesTotalEl.textContent = formatNumber(total);
  }
  if (chargesRangeWindow) {
    if (!timeline || timeline.length === 0) {
      chargesRangeWindow.textContent = 'in this period';
      return;
    }
    if (timeline.length === 1) {
      const single = timeline[0];
      if (granularity === 'hour') {
        chargesRangeWindow.textContent = `at ${formatDateTime(single?.start)}`;
      } else {
        const label = single?.start || single?.day;
        chargesRangeWindow.textContent = `on ${formatDate(label)}`;
      }
      return;
    }
    const start = timeline[0]?.start || timeline[0]?.day;
    const lastEntry = timeline[timeline.length - 1];
    const end = lastEntry?.start || lastEntry?.day;
    const rangeText = formatPeriodRange(start, end, granularity);
    chargesRangeWindow.textContent = rangeText ? `between ${rangeText}` : 'in this period';
  }
};

const updateChargesChart = (series, granularity = 'day') => {
  const resolvedGranularity = granularity === 'hour' ? 'hour' : 'day';
  const timeline = (series || [])
    .map((entry) => {
      const baseStart = entry?.start || (entry?.day ? `${entry.day}T00:00:00` : null);
      if (!baseStart) {
        return null;
      }
      const sessions = Number.isFinite(entry?.sessions) ? entry.sessions : 0;
      const label =
        resolvedGranularity === 'hour'
          ? formatDateTime(baseStart, { includeWeekday: false })
          : formatDate(entry?.day || baseStart, { includeWeekday: false });
      return {
        start: baseStart,
        end: entry?.end || null,
        day: entry?.day,
        sessions,
        label,
      };
    })
    .filter(Boolean);

  updateChargesSummary(timeline, resolvedGranularity);

  if (!chargesChartCanvas) {
    return;
  }

  if (!window.Chart) {
    setChartStatus('Chart library failed to load.');
    return;
  }

  if (timeline.length === 0) {
    if (chargesChart) {
      chargesChart.destroy();
      chargesChart = undefined;
    }
    setChartStatus('No charging activity recorded yet.');
    return;
  }

  const labels = timeline.map((entry) => entry.label);
  const values = timeline.map((entry) => entry.sessions);
  const isHourly = resolvedGranularity === 'hour';
  const style = {
    borderWidth: isHourly ? 2 : 3,
    tension: isHourly ? 0.3 : 0.45,
    pointRadius: isHourly ? 0 : 5,
    pointHoverRadius: isHourly ? 4 : 7,
  };

  if (chargesChart) {
    chargesChart.data.labels = labels;
    const dataset = chargesChart.data.datasets[0];
    dataset.data = values;
    dataset.borderColor = '#0ea5e9';
    dataset.borderWidth = style.borderWidth;
    dataset.tension = style.tension;
    dataset.fill = 'origin';
    dataset.pointBackgroundColor = '#38bdf8';
    dataset.pointBorderWidth = 0;
    dataset.pointRadius = style.pointRadius;
    dataset.pointHoverRadius = style.pointHoverRadius;
    const tickOptions = chargesChart.options.scales?.x?.ticks;
    if (tickOptions) {
      if (isHourly) {
        tickOptions.maxTicksLimit = 12;
      } else {
        delete tickOptions.maxTicksLimit;
      }
    }
    const tooltipCallbacks = chargesChart.options.plugins?.tooltip?.callbacks;
    if (tooltipCallbacks) {
      tooltipCallbacks.title = (items) => {
        if (!items?.length) {
          return '';
        }
        const { chart, dataIndex } = items[0];
        return formatTooltipTitle(chart, dataIndex);
      };
    }
    chargesChart.$timeline = timeline;
    chargesChart.$granularity = resolvedGranularity;
    chargesChart.update();
  } else {
    const context = chargesChartCanvas.getContext('2d');
    chargesChart = new window.Chart(context, {
      type: 'line',
      data: {
        labels,
        datasets: [
          {
            label: 'Charges',
            data: values,
            borderColor: '#0ea5e9',
            borderWidth: style.borderWidth,
            tension: style.tension,
            fill: 'origin',
            backgroundColor(context) {
              const { chart } = context;
              const { ctx, chartArea } = chart;
              if (!chartArea) {
                return null;
              }
              const gradient = ctx.createLinearGradient(0, chartArea.bottom, 0, chartArea.top);
              gradient.addColorStop(0, 'rgba(14, 165, 233, 0)');
              gradient.addColorStop(1, 'rgba(14, 165, 233, 0.45)');
              return gradient;
            },
            pointBackgroundColor: '#38bdf8',
            pointBorderWidth: 0,
            pointRadius: style.pointRadius,
            pointHoverRadius: style.pointHoverRadius,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: false,
          tooltip: {
            backgroundColor: '#0f172a',
            titleColor: '#f8fafc',
            bodyColor: '#e2e8f0',
            borderWidth: 0,
            padding: 12,
            displayColors: false,
            callbacks: {
              title(items) {
                if (!items?.length) {
                  return '';
                }
                const { chart, dataIndex } = items[0];
                return formatTooltipTitle(chart, dataIndex);
              },
              label(item) {
                return `${formatNumber(item.parsed.y)} charges`;
              },
            },
          },
          chargesGlow: {
            enabled: true,
          },
        },
        scales: {
          x: {
            grid: {
              display: false,
            },
            ticks: {
              color: '#475569',
              maxRotation: 0,
              autoSkip: true,
              maxTicksLimit: isHourly ? 12 : undefined,
            },
          },
          y: {
            beginAtZero: true,
            grid: {
              color: 'rgba(148, 163, 184, 0.2)',
              drawTicks: false,
            },
            ticks: {
              color: '#475569',
              padding: 8,
              precision: 0,
            },
          },
        },
      },
    });
  }

  chargesChart.$timeline = timeline;
  chargesChart.$granularity = resolvedGranularity;
  setChartStatus(null);
};

const updateHighChargingLocations = (locations) => {
  if (!highChargingListEl || !highChargingNoteEl) {
    return;
  }

  highChargingListEl.innerHTML = '';
  highChargingListEl.hidden = false;
  highChargingNoteEl.hidden = false;

  if (!Array.isArray(locations)) {
    highChargingNoteEl.textContent = 'Location utilization data unavailable.';
    highChargingListEl.hidden = true;
    return;
  }

  const highlighted = locations
    .map((entry) => {
      const percent =
        typeof entry?.active_charging_utilization_pct === 'number'
          ? entry.active_charging_utilization_pct
          : Number(entry?.active_charging_utilization_pct);
      const stationCount =
        typeof entry?.station_count === 'number' ? entry.station_count : Number(entry?.station_count);
      const portCount =
        typeof entry?.port_count === 'number' ? entry.port_count : Number(entry?.port_count);
      return {
        id: entry?.location_id,
        percent,
        stationCount,
        portCount,
      };
    })
    .filter((entry) => entry.id && Number.isFinite(entry.percent) && entry.percent > 30)
    .sort((a, b) => b.percent - a.percent);

  if (highlighted.length === 0) {
    highChargingNoteEl.textContent = 'No locations currently exceed the 30% charging rate threshold.';
    highChargingListEl.hidden = true;
    return;
  }

  highChargingNoteEl.textContent =
    'These locations exceed 30% active charging utilization. Consider expanding capacity.';

  highlighted.forEach((entry) => {
    const item = document.createElement('li');
    item.className = 'high-charging-item';

    const details = document.createElement('div');
    details.className = 'high-charging-details';

    if (entry.id) {
      const link = document.createElement('a');
      link.href = `location.html?id=${encodeURIComponent(entry.id)}`;
      link.textContent = entry.id;
      link.setAttribute('aria-label', `View utilization details for location ${entry.id}`);
      details.appendChild(link);
    } else {
      const fallback = document.createElement('span');
      fallback.textContent = 'Unknown location';
      details.appendChild(fallback);
    }

    const metaParts = [];
    if (Number.isFinite(entry.stationCount) && entry.stationCount > 0) {
      metaParts.push(`${formatNumber(entry.stationCount)} stations`);
    }
    if (Number.isFinite(entry.portCount) && entry.portCount > 0) {
      metaParts.push(`${formatNumber(entry.portCount)} ports`);
    }
    if (metaParts.length > 0) {
      const meta = document.createElement('span');
      meta.className = 'high-charging-meta';
      meta.textContent = metaParts.join(' · ');
      details.appendChild(meta);
    }

    const value = document.createElement('strong');
    value.textContent = formatPercent(entry.percent, 1);

    item.append(details, value);
    highChargingListEl.appendChild(item);
  });
};

const updateProblematic = (problematic, locations) => {
  const tbody = document.getElementById('problematic-table');
  const countEl = document.getElementById('problematic-count');
  if (!tbody || !countEl) return;
  tbody.innerHTML = '';
  if (!problematic || problematic.length === 0) {
    countEl.textContent = 'All chargers look healthy right now.';
    return;
  }
  countEl.textContent = `${problematic.length} chargers require attention.`;
  problematic.forEach((entry) => {
    const tr = document.createElement('tr');
    const cells = [
      entry.location_id || '–',
      entry.station_id || '–',
      entry.port_id || '–',
      entry.status || '–',
      entry.reason || '–',
    ];
    cells.forEach((value, index) => {
      const td = document.createElement('td');
      td.textContent = value;
      if (index === 0 && entry.location_id && locations?.[entry.location_id]) {
        const coords = locations[entry.location_id];
        const link = document.createElement('a');
        link.href = `https://www.openstreetmap.org/?mlat=${coords.lat}&mlon=${coords.lon}#map=18/${coords.lat}/${coords.lon}`;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        link.textContent = value;
        td.innerHTML = '';
        td.appendChild(link);
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
};

const showError = (error) => {
  console.error(error);
  const countEl = document.getElementById('problematic-count');
  if (countEl) {
    countEl.textContent = 'Unable to load data from the backend.';
    countEl.classList.add('muted');
  }
  const utilNote = document.getElementById('utilization-note');
  if (utilNote) {
    utilNote.textContent = 'Unable to load utilization metrics.';
  }
  const meta = document.getElementById('summary-meta');
  if (meta) {
    meta.textContent = 'Backend unavailable';
  }
  setUtilizationData([], [], []);
  updateHighChargingLocations(null);
  setHeatmapData([], null);
};

const setLocationLoading = (message) => {
  if (!locationLoadingEl) {
    return;
  }
  if (message) {
    locationLoadingEl.textContent = message;
    locationLoadingEl.hidden = false;
  } else {
    locationLoadingEl.hidden = true;
  }
};

const setLocationError = (message) => {
  if (!locationErrorEl) {
    return;
  }
  if (message) {
    locationErrorEl.textContent = message;
    locationErrorEl.hidden = false;
  } else {
    locationErrorEl.hidden = true;
  }
};

const fingerprintCellKey = (weekday, hour) => `${weekday}-${hour}`;

const fingerprintColorForValue = (value) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return 'rgba(148, 163, 184, 0.18)';
  }
  const ratio = Math.min(Math.max(numeric, 0), 100) / 100;
  const hue = 210 - ratio * 160;
  const saturation = 70 + ratio * 20;
  const lightness = 92 - ratio * 55;
  return `hsl(${hue.toFixed(1)}deg, ${saturation.toFixed(1)}%, ${lightness.toFixed(1)}%)`;
};

const formatFingerprintHourTick = (hour) => {
  const numeric = Number(hour);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return '';
  }
  return numeric.toString().padStart(2, '0');
};

const formatFingerprintHourLabel = (hour) => {
  const tick = formatFingerprintHourTick(hour);
  if (tick) {
    return `${tick}:00`;
  }
  return FINGERPRINT_HOUR_LABELS[hour] || `${hour}:00`;
};

const formatFingerprintCellLabel = (weekday, hour) => {
  const day = FINGERPRINT_WEEKDAY_LABELS[weekday] ?? `Day ${weekday}`;
  const hourLabel = formatFingerprintHourLabel(hour);
  return `${day} ${hourLabel}`;
};

const describeStationMeta = (station) => {
  if (!station) {
    return '';
  }
  const parts = [];
  const portCount = Number(station.port_count);
  if (Number.isFinite(portCount) && portCount > 0) {
    parts.push(`${formatNumber(portCount)} port${portCount === 1 ? '' : 's'}`);
  }
  const weekSummary = station.summary?.week ?? {};
  const occupationPct = Number(weekSummary.occupation_utilization_pct);
  if (Number.isFinite(occupationPct) && occupationPct > 0) {
    parts.push(`${formatPercent(occupationPct, 0)} occupied`);
  }
  const availabilityRatio = Number(weekSummary.availability_ratio);
  if (Number.isFinite(availabilityRatio) && availabilityRatio > 0) {
    parts.push(`${formatRatioPercent(availabilityRatio, 0)} availability`);
  }
  const monitoredDays = Number(weekSummary.monitored_days);
  if (Number.isFinite(monitoredDays) && monitoredDays > 0) {
    const formatted = formatDecimal(monitoredDays, {
      minimumFractionDigits: 0,
      maximumFractionDigits: 1,
    });
    if (formatted !== '–') {
      parts.push(`${formatted} days monitored`);
    }
  }
  return parts.join(' · ');
};

const createStationHeatmapCard = (station) => {
  const card = document.createElement('article');
  card.className = 'card station-heatmap-card';
  const body = document.createElement('div');
  body.className = 'card__body';
  card.appendChild(body);

  const header = document.createElement('div');
  header.className = 'station-heatmap-header';
  body.appendChild(header);

  const title = document.createElement('h4');
  title.className = 'station-heatmap-title';
  const stationId = station?.station_id ? String(station.station_id) : '';
  title.textContent = stationId ? `Station ${stationId}` : 'Station';
  header.appendChild(title);

  const portCount = Number(station?.port_count);
  if (Number.isFinite(portCount) && portCount > 0) {
    const pill = document.createElement('span');
    pill.className = 'station-heatmap-pill';
    pill.textContent = `${formatNumber(portCount)} port${portCount === 1 ? '' : 's'}`;
    header.appendChild(pill);
  }

  const meta = document.createElement('p');
  meta.className = 'station-heatmap-meta muted';
  const metaText = describeStationMeta(station);
  if (metaText) {
    meta.textContent = metaText;
    meta.dataset.metaState = 'provided';
  } else {
    meta.textContent = 'Utilization summary will appear as telemetry accumulates.';
    meta.dataset.metaState = 'pending';
  }
  body.appendChild(meta);

  const generated = document.createElement('p');
  generated.className = 'station-heatmap-generated muted';
  generated.hidden = true;
  body.appendChild(generated);

  const statusEl = document.createElement('p');
  statusEl.className = 'station-heatmap-status muted';
  statusEl.textContent = 'Loading fingerprint…';
  body.appendChild(statusEl);

  const visual = document.createElement('div');
  visual.className = 'station-heatmap-visual is-loading';
  visual.setAttribute('role', 'img');
  visual.setAttribute('aria-label', 'Weekly station fingerprint heatmap');
  body.appendChild(visual);

  const coverageEl = document.createElement('p');
  coverageEl.className = 'station-heatmap-coverage muted';
  coverageEl.hidden = true;
  body.appendChild(coverageEl);

  const legend = document.createElement('div');
  legend.className = 'station-heatmap-legend';
  const legendMin = document.createElement('span');
  legendMin.textContent = '0%';
  const legendBar = document.createElement('div');
  legendBar.className = 'station-heatmap-legend-bar';
  const legendMax = document.createElement('span');
  legendMax.textContent = '100%';
  legend.append(legendMin, legendBar, legendMax);
  body.appendChild(legend);

  return {
    card,
    statusEl,
    visual,
    legendMinEl: legendMin,
    legendMaxEl: legendMax,
    coverageEl,
    generatedEl: generated,
    metaEl: meta,
  };
};

const renderStationFingerprint = (card, fingerprint) => {
  if (!card) {
    return;
  }
  const {
    visual,
    statusEl,
    legendMinEl,
    legendMaxEl,
    coverageEl,
    generatedEl,
    metaEl,
  } = card;
  if (visual) {
    visual.innerHTML = '';
    visual.classList.remove('is-loading');
  }
  if (statusEl) {
    statusEl.textContent = '';
    statusEl.hidden = true;
  }

  const cells = Array.isArray(fingerprint?.cells) ? fingerprint.cells : [];
  if (cells.length === 0) {
    if (statusEl) {
      statusEl.textContent = 'No fingerprint data available yet.';
      statusEl.hidden = false;
    }
    return;
  }

  const grid = document.createElement('div');
  grid.className = 'fingerprint-grid';
  const corner = document.createElement('div');
  corner.className = 'fingerprint-grid__corner';
  grid.appendChild(corner);

  FINGERPRINT_WEEKDAY_SHORT.forEach((label, index) => {
    const dayCell = document.createElement('div');
    dayCell.className = 'fingerprint-grid__day';
    dayCell.textContent = label ?? `Day ${index}`;
    grid.appendChild(dayCell);
  });

  const cellMap = new Map();
  let coverageSum = 0;
  let coverageCount = 0;
  cells.forEach((cell) => {
    const key = fingerprintCellKey(cell.weekday, cell.hour);
    cellMap.set(key, cell);
    const coverage = Number(cell.coverage_ratio);
    if (Number.isFinite(coverage) && coverage > 0) {
      coverageSum += coverage;
      coverageCount += 1;
    }
  });

  let minValue = Number.POSITIVE_INFINITY;
  let maxValue = Number.NEGATIVE_INFINITY;

  for (let hour = 0; hour < FINGERPRINT_HOUR_LABELS.length; hour += 1) {
    const hourLabel = document.createElement('div');
    hourLabel.className = 'fingerprint-grid__hour';
    hourLabel.textContent = formatFingerprintHourTick(hour) || FINGERPRINT_HOUR_LABELS[hour] || `${hour}:00`;
    grid.appendChild(hourLabel);

    for (let weekday = 0; weekday < FINGERPRINT_WEEKDAY_SHORT.length; weekday += 1) {
      const cellData = cellMap.get(fingerprintCellKey(weekday, hour));
      const occupancy = Number(cellData?.metrics?.occupation_utilization_pct ?? 0);
      const coverage = Number(cellData?.coverage_ratio ?? 0);
      const cellEl = document.createElement('div');
      cellEl.className = 'fingerprint-grid__cell';
      const color = fingerprintColorForValue(occupancy);
      cellEl.style.setProperty('--heat-color', color);
      cellEl.style.background = color;
      if (coverage > 0 && coverage < 0.75) {
        cellEl.classList.add(coverage < 0.4 ? 'is-low-coverage' : 'is-medium-coverage');
      }
      const label = cellData?.label || formatFingerprintCellLabel(weekday, hour);
      const occupancyLabel = formatPercent(occupancy, 0);
      const coverageLabel = Number.isFinite(coverage) && coverage > 0 ? formatRatioPercent(coverage, 0) : null;
      cellEl.title = coverageLabel
        ? `${label} · ${occupancyLabel} occupied · ${coverageLabel} coverage`
        : `${label} · ${occupancyLabel} occupied`;
      cellEl.dataset.value = String(Math.round(Math.min(Math.max(occupancy, 0), 100)));
      if (Number.isFinite(occupancy)) {
        minValue = Math.min(minValue, occupancy);
        maxValue = Math.max(maxValue, occupancy);
      }
      grid.appendChild(cellEl);
    }
  }

  if (visual) {
    visual.appendChild(grid);
  }
  if (legendMinEl) {
    legendMinEl.textContent = Number.isFinite(minValue) ? formatPercent(minValue, 0) : '0%';
  }
  if (legendMaxEl) {
    legendMaxEl.textContent = Number.isFinite(maxValue) ? formatPercent(maxValue, 0) : '100%';
  }
  if (coverageEl) {
    if (coverageCount > 0) {
      const averageCoverage = coverageSum / coverageCount;
      coverageEl.textContent = `Average coverage ${formatRatioPercent(averageCoverage, 0)}`;
      coverageEl.hidden = false;
    } else {
      coverageEl.textContent = '';
      coverageEl.hidden = true;
    }
  }
  if (generatedEl) {
    const generatedText = fingerprint?.generated ? formatDateTime(fingerprint.generated) : null;
    if (generatedText && generatedText !== '–') {
      generatedEl.textContent = `Fingerprint generated ${generatedText}`;
      generatedEl.hidden = false;
    } else {
      generatedEl.textContent = '';
      generatedEl.hidden = true;
    }
  }
  if (metaEl && metaEl.dataset.metaState === 'pending') {
    const portCount = Number(fingerprint?.port_count);
    if (Number.isFinite(portCount) && portCount > 0) {
      metaEl.textContent = `${formatNumber(portCount)} port${portCount === 1 ? '' : 's'} monitored.`;
      metaEl.dataset.metaState = 'provided';
    }
  }
};

const updateStationHeatmapStatusSummary = () => {
  if (!stationHeatmapStatus) {
    return;
  }
  if (stationHeatmapPending > 0) {
    const total = stationHeatmapCards.size;
    const loaded = Math.max(total - stationHeatmapPending, 0);
    stationHeatmapStatus.textContent = `Loading station fingerprints… (${formatNumber(loaded)} of ${formatNumber(total)} ready)`;
    return;
  }
  if (stationHeatmapCards.size > 0) {
    const total = stationHeatmapCards.size;
    stationHeatmapStatus.textContent = `Showing weekly fingerprints for ${formatNumber(total)} station${total === 1 ? '' : 's'}.`;
  } else {
    stationHeatmapStatus.textContent = 'No station fingerprint data available yet.';
  }
};

const loadStationFingerprint = async (sequence, locationId, station, card) => {
  const stationIdRaw = station?.station_id ?? station;
  const stationId = stationIdRaw === null || stationIdRaw === undefined ? null : String(stationIdRaw).trim();
  if (!stationId) {
    if (card?.statusEl) {
      card.statusEl.textContent = 'Station identifier unavailable.';
      card.statusEl.hidden = false;
    }
    if (card?.visual) {
      card.visual.classList.remove('is-loading');
    }
    stationHeatmapPending = Math.max(0, stationHeatmapPending - 1);
    updateStationHeatmapStatusSummary();
    return;
  }
  try {
    const response = await fetch(
      `${API_BASE}/stations/${encodeURIComponent(locationId)}/${encodeURIComponent(stationId)}/fingerprint`,
    );
    if (sequence !== stationHeatmapSequence) {
      return;
    }
    if (response.status === 404) {
      if (card?.visual) {
        card.visual.classList.remove('is-loading');
      }
      if (card?.statusEl) {
        card.statusEl.textContent = 'Not enough telemetry to build this fingerprint yet.';
        card.statusEl.hidden = false;
      }
      return;
    }
    if (!response.ok) {
      throw new Error(`Backend returned ${response.status}`);
    }
    const fingerprint = await response.json();
    if (sequence !== stationHeatmapSequence) {
      return;
    }
    renderStationFingerprint(card, fingerprint);
  } catch (error) {
    if (sequence !== stationHeatmapSequence) {
      return;
    }
    console.error(error);
    if (card?.visual) {
      card.visual.classList.remove('is-loading');
    }
    if (card?.statusEl) {
      card.statusEl.textContent = 'Unable to load fingerprint.';
      card.statusEl.hidden = false;
    }
  } finally {
    if (sequence !== stationHeatmapSequence) {
      return;
    }
    stationHeatmapPending = Math.max(0, stationHeatmapPending - 1);
    updateStationHeatmapStatusSummary();
  }
};

const resetStationHeatmaps = (message) => {
  stationHeatmapSequence += 1;
  stationHeatmapPending = 0;
  stationHeatmapCards.clear();
  if (stationHeatmapContainer) {
    stationHeatmapContainer.innerHTML = '';
  }
  if (stationHeatmapStatus && message !== undefined) {
    stationHeatmapStatus.textContent = message;
  }
  return stationHeatmapSequence;
};

const updateStationHeatmaps = (locationId, stations) => {
  if (!stationHeatmapSection || !stationHeatmapContainer || !stationHeatmapStatus) {
    return;
  }
  const sequence = resetStationHeatmaps('Loading station fingerprints…');
  const stationList = Array.isArray(stations)
    ? stations.filter((entry) => entry && entry.station_id !== null && entry.station_id !== undefined)
    : [];
  if (stationList.length === 0) {
    stationHeatmapStatus.textContent = 'Station fingerprint heatmaps will appear once telemetry is available.';
    return;
  }
  stationHeatmapPending = stationList.length;
  stationList.forEach((station) => {
    const card = createStationHeatmapCard(station);
    stationHeatmapContainer.appendChild(card.card);
    stationHeatmapCards.set(String(station.station_id), card);
    loadStationFingerprint(sequence, locationId, station, card);
  });
  updateStationHeatmapStatusSummary();
};

const renderLocationUsageChart = (chart, canvas, statusEl, timeline, granularity) => {
  if (!canvas) {
    if (statusEl) {
      statusEl.hidden = true;
    }
    return chart;
  }

  if (!window.Chart) {
    if (statusEl) {
      statusEl.textContent = 'Chart library failed to load.';
      statusEl.hidden = false;
    }
    return chart;
  }

  const points = (timeline || [])
    .map((entry) => {
      if (!entry?.start) {
        return null;
      }
      const occupancy = Number(entry.occupation_utilization_pct);
      if (!Number.isFinite(occupancy)) {
        return null;
      }
      const label =
        granularity === 'hour'
          ? formatDateTime(entry.start, { includeWeekday: false })
          : formatDate(entry.start, { includeWeekday: true });
      return {
        start: entry.start,
        end: entry.end,
        value: occupancy,
        availability: Number(entry.availability_ratio ?? Number.NaN),
        label,
      };
    })
    .filter(Boolean);

  if (points.length === 0) {
    if (chart) {
      chart.destroy();
    }
    if (statusEl) {
      const hasTimeline = Array.isArray(timeline) && timeline.length > 0;
      statusEl.textContent = hasTimeline
        ? 'No usage recorded for this period yet.'
        : 'Usage timeline unavailable.';
      statusEl.hidden = false;
    }
    return undefined;
  }

  if (statusEl) {
    statusEl.hidden = true;
  }

  const labels = points.map((point) => point.label);
  const values = points.map((point) => point.value);
  const style = granularity === 'hour'
    ? { tension: 0.35, pointRadius: 0, pointHoverRadius: 4 }
    : { tension: 0.4, pointRadius: 3, pointHoverRadius: 6 };

  if (chart) {
    chart.data.labels = labels;
    chart.data.datasets[0].data = values;
    chart.options.scales.y.max = 100;
    const tickOptions = chart.options.scales?.x?.ticks;
    if (tickOptions) {
      tickOptions.maxTicksLimit = granularity === 'hour' ? 12 : undefined;
    }
    chart.update();
    chart.$points = points;
    chart.$granularity = granularity;
    return chart;
  }

  const context = canvas.getContext('2d');
  const newChart = new window.Chart(context, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'Occupied',
          data: values,
          borderColor: '#0ea5e9',
          borderWidth: 2,
          tension: style.tension,
          fill: 'origin',
          backgroundColor(ctx) {
            const { chart: chartCtx } = ctx;
            const { ctx: canvasCtx, chartArea } = chartCtx;
            if (!chartArea) {
              return null;
            }
            const gradient = canvasCtx.createLinearGradient(0, chartArea.bottom, 0, chartArea.top);
            gradient.addColorStop(0, 'rgba(14, 165, 233, 0)');
            gradient.addColorStop(1, 'rgba(14, 165, 233, 0.35)');
            return gradient;
          },
          pointBackgroundColor: '#38bdf8',
          pointBorderWidth: 0,
          pointRadius: style.pointRadius,
          pointHoverRadius: style.pointHoverRadius,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: false,
        tooltip: {
          backgroundColor: '#0f172a',
          titleColor: '#f8fafc',
          bodyColor: '#e2e8f0',
          displayColors: false,
          callbacks: {
            title(items) {
              if (!items?.length) {
                return '';
              }
              const index = items[0].dataIndex;
              return points[index]?.label ?? '';
            },
            label(item) {
              const occupied = formatDecimal(item.parsed.y, {
                minimumFractionDigits: 1,
                maximumFractionDigits: 1,
              });
              return `${occupied}% occupied`;
            },
          },
        },
      },
      scales: {
        x: {
          grid: {
            display: false,
          },
          ticks: {
            color: '#475569',
            maxRotation: 0,
            autoSkip: true,
            maxTicksLimit: granularity === 'hour' ? 12 : undefined,
          },
        },
        y: {
          beginAtZero: true,
          max: 100,
          grid: {
            color: 'rgba(148, 163, 184, 0.2)',
            drawTicks: false,
          },
          ticks: {
            color: '#475569',
            padding: 8,
            callback(value) {
              return `${value}%`;
            },
          },
        },
      },
    },
  });
  newChart.$points = points;
  newChart.$granularity = granularity;
  return newChart;
};

const invalidateLocationMapSize = () => {
  if (locationMap) {
    if (typeof locationMap.resize === 'function') {
      locationMap.resize();
    }
  }
};

const clearLocationMapResizeTimer = () => {
  if (!locationMapResizeTimer) {
    return;
  }
  if (locationMapResizeTimerType === 'raf' && typeof cancelAnimationFrame === 'function') {
    cancelAnimationFrame(locationMapResizeTimer);
  } else if (locationMapResizeTimerType === 'timeout') {
    clearTimeout(locationMapResizeTimer);
  }
  locationMapResizeTimer = undefined;
  locationMapResizeTimerType = undefined;
};

function runLocationMapResize() {
  locationMapResizeTimer = undefined;
  locationMapResizeTimerType = undefined;
  invalidateLocationMapSize();
  if (locationMapResizeRepeatCount > 0) {
    locationMapResizeRepeatCount -= 1;
    queueLocationMapResize();
  }
}

function queueLocationMapResize() {
  if (typeof requestAnimationFrame === 'function') {
    locationMapResizeTimerType = 'raf';
    locationMapResizeTimer = requestAnimationFrame(runLocationMapResize);
    return;
  }
  locationMapResizeTimerType = 'timeout';
  locationMapResizeTimer = setTimeout(runLocationMapResize, 30);
}

const scheduleLocationMapResize = (additionalFrames = 2) => {
  if (!locationMap) {
    return;
  }
  locationMapResizeRepeatCount = Math.max(locationMapResizeRepeatCount, Math.max(0, additionalFrames));
  if (!locationMapResizeTimer) {
    queueLocationMapResize();
  }
};

const attachLocationMapResizeObserver = () => {
  if (!locationMapContainer || typeof ResizeObserver !== 'function') {
    return;
  }
  if (locationMapResizeObserver) {
    return;
  }
  locationMapResizeObserver = new ResizeObserver(() => {
    scheduleLocationMapResize(2);
  });
  locationMapResizeObserver.observe(locationMapContainer);
};

const detachLocationMapResizeObserver = () => {
  if (!locationMapResizeObserver) {
    return;
  }
  locationMapResizeObserver.disconnect();
  locationMapResizeObserver = undefined;
};

const formatPortStatusLabel = (status) => {
  if (!status) {
    return 'Unknown';
  }
  return String(status)
    .trim()
    .split(/[_\s]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(' ');
};

const getStatusBadgeClass = (status) => {
  const classes = ['badge'];
  if (!status) {
    return classes.join(' ');
  }
  const normalized = String(status).trim().toUpperCase();
  if (['AVAILABLE', 'OPERATIVE', 'FREE', 'LIBRE'].includes(normalized)) {
    classes.push('badge--success');
  } else if (['IN_USE', 'CHARGING', 'OCCUPIED', 'RESERVED'].includes(normalized)) {
    classes.push('badge--warning');
  } else if (
    [
      'OUT_OF_SERVICE',
      'UNAVAILABLE',
      'ERROR',
      'FAULT',
      'TEMPORARILY_UNAVAILABLE',
      'MAINTENANCE',
    ].includes(normalized)
  ) {
    classes.push('badge--danger');
  }
  return classes.join(' ');
};

const setNearMeStatus = (message) => {
  if (nearMeStatusEl) {
    nearMeStatusEl.textContent = message || '';
  }
};

const clearNearMeError = () => {
  if (nearMeErrorEl) {
    nearMeErrorEl.textContent = '';
    nearMeErrorEl.hidden = true;
  }
};

const showNearMeError = (message) => {
  if (!nearMeErrorEl) {
    return;
  }
  nearMeErrorEl.textContent = message || 'Unable to load nearby chargers.';
  nearMeErrorEl.hidden = false;
};

const setNearMeLoading = (isLoading) => {
  if (nearMeSection) {
    nearMeSection.setAttribute('aria-busy', isLoading ? 'true' : 'false');
  }
  if (nearMeLocateButton) {
    nearMeLocateButton.disabled = Boolean(isLoading);
  }
};

const renderNearMeResults = (locations) => {
  if (!nearMeResultsEl) {
    return;
  }
  nearMeResultsEl.innerHTML = '';
  if (!Array.isArray(locations) || locations.length === 0) {
    const empty = document.createElement('p');
    empty.className = 'muted';
    empty.textContent = 'No chargers were found near your location.';
    nearMeResultsEl.appendChild(empty);
    return;
  }

  locations.forEach((location) => {
    const card = document.createElement('article');
    card.className = 'card near-me-card';
    const body = document.createElement('div');
    body.className = 'card__body';

    const header = document.createElement('div');
    header.className = 'near-me-card__header';
    const title = document.createElement('h3');
    title.className = 'near-me-card__title';
    title.textContent = location?.address || `Location ${location?.location_id ?? ''}`.trim();
    header.appendChild(title);

    const distanceBadge = document.createElement('span');
    distanceBadge.className = 'badge near-me-distance';
    distanceBadge.textContent = formatDistance(location?.distance_m);
    header.appendChild(distanceBadge);
    body.appendChild(header);

    const idLine = document.createElement('p');
    idLine.className = 'near-me-card__id muted';
    idLine.textContent = `Location ${location?.location_id ?? 'unknown'}`;
    body.appendChild(idLine);

    const stationCount = Number(location?.station_count);
    const portCount = Number(location?.port_count);
    const countParts = [];
    if (Number.isFinite(stationCount)) {
      countParts.push(`${formatNumber(stationCount)} station${stationCount === 1 ? '' : 's'}`);
    }
    if (Number.isFinite(portCount)) {
      countParts.push(`${formatNumber(portCount)} port${portCount === 1 ? '' : 's'}`);
    }
    if (countParts.length > 0) {
      const countsLine = document.createElement('div');
      countsLine.className = 'near-me-card__meta';
      countsLine.textContent = countParts.join(' · ');
      body.appendChild(countsLine);
    }

    const chargerParts = [];
    const chargerTypeLabel = formatChargerType(location?.charger_type);
    if (chargerTypeLabel) {
      chargerParts.push(chargerTypeLabel);
    }
    const maxPowerLabel = formatPowerKw(location?.max_power_kw);
    if (maxPowerLabel) {
      chargerParts.push(`Up to ${maxPowerLabel}`);
    }
    if (chargerParts.length > 0) {
      const chargerLine = document.createElement('div');
      chargerLine.className = 'near-me-card__meta near-me-card__meta--secondary';
      chargerLine.textContent = chargerParts.join(' · ');
      body.appendChild(chargerLine);
    }

    if (location?.updated) {
      const updatedLine = document.createElement('p');
      updatedLine.className = 'near-me-card__updated muted';
      updatedLine.textContent = `Updated ${formatDateTime(location.updated, {
        includeWeekday: false,
      })}`;
      body.appendChild(updatedLine);
    }

    const ports = Array.isArray(location?.ports) ? location.ports : [];
    if (ports.length > 0) {
      const list = document.createElement('ul');
      list.className = 'near-me-port-list';
      ports.forEach((port) => {
        const item = document.createElement('li');
        item.className = 'near-me-port';

        const label = document.createElement('span');
        label.className = 'near-me-port__id';
        const parts = [];
        if (port?.station_id) {
          parts.push(`Station ${port.station_id}`);
        }
        if (port?.port_id) {
          parts.push(`Port ${port.port_id}`);
        }
        label.textContent = parts.join(' · ') || 'Charger';
        item.appendChild(label);

        const statusBadge = document.createElement('span');
        statusBadge.className = `${getStatusBadgeClass(port?.status)} near-me-port__status`;
        statusBadge.textContent = formatPortStatusLabel(port?.status);
        item.appendChild(statusBadge);

        if (port?.last_updated) {
          const updated = document.createElement('span');
          updated.className = 'near-me-port__updated muted';
          updated.textContent = `Updated ${formatDateTime(port.last_updated, {
            includeWeekday: false,
          })}`;
          item.appendChild(updated);
        }

        list.appendChild(item);
      });
      body.appendChild(list);
    } else {
      const emptyPorts = document.createElement('p');
      emptyPorts.className = 'muted';
      emptyPorts.textContent = 'No telemetry is available for this location yet.';
      body.appendChild(emptyPorts);
    }

    card.appendChild(body);
    nearMeResultsEl.appendChild(card);
  });
};

const geolocationErrorMessage = (error) => {
  if (!error || typeof error.code !== 'number') {
    return 'Unable to determine your location.';
  }
  switch (error.code) {
    case error.PERMISSION_DENIED:
      return 'Location access was denied. Enable location permissions to find chargers near you.';
    case error.POSITION_UNAVAILABLE:
      return 'We could not determine your location. Try again in an area with better signal.';
    case error.TIMEOUT:
      return 'Timed out while waiting for your location. Please try again.';
    default:
      return 'Unable to determine your location.';
  }
};

const requestUserLocation = () =>
  new Promise((resolve, reject) => {
    if (!navigator?.geolocation) {
      reject(new Error('Geolocation is not supported by this browser.'));
      return;
    }
    navigator.geolocation.getCurrentPosition(
      (position) => {
        resolve(position.coords);
      },
      (error) => {
        reject(error);
      },
      {
        enableHighAccuracy: true,
        timeout: 15000,
        maximumAge: 0,
      }
    );
  });

const fetchNearbyLocations = async (
  lat,
  lon,
  limit = NEAR_ME_DEFAULT_LIMIT,
  includeMotorcycle = false,
  highPowerOnly = false,
) => {
  const params = new URLSearchParams({
    lat: String(lat),
    lon: String(lon),
    limit: String(limit),
  });
  if (includeMotorcycle) {
    params.set('include_motorcycle', '1');
  }
  if (highPowerOnly) {
    params.set('high_power_only', '1');
  }
  const response = await fetch(`${API_BASE}/nearby?${params.toString()}`, {
    headers: { Accept: 'application/json' },
  });
  if (!response.ok) {
    throw new Error(`Request failed with status ${response.status}`);
  }
  return response.json();
};

const requestNearbyChargers = async () => {
  if (!nearMeSection || nearMeRequestInFlight) {
    return;
  }
  if (!navigator?.geolocation) {
    showNearMeError('Geolocation is not supported by your browser.');
    setNearMeStatus('Unable to access location services.');
    if (nearMeLocateButton) {
      nearMeLocateButton.disabled = true;
    }
    return;
  }

  nearMeRequestInFlight = true;
  setNearMeLoading(true);
  clearNearMeError();
  setNearMeStatus('Requesting your location…');

  try {
    const coords = await requestUserLocation();
    setNearMeStatus('Searching for chargers near you…');
    const includeMotorcycle = Boolean(nearMeIncludeMotorcycleInput?.checked);
    const highPowerOnly = Boolean(nearMeHighPowerOnlyInput?.checked);
    const payload = await fetchNearbyLocations(
      coords.latitude,
      coords.longitude,
      NEAR_ME_DEFAULT_LIMIT,
      includeMotorcycle,
      highPowerOnly,
    );
    const locations = Array.isArray(payload?.locations) ? payload.locations : [];
    renderNearMeResults(locations);
    if (locations.length > 0) {
      const farthest = locations[locations.length - 1];
      const distance = formatDistance(farthest?.distance_m);
      const countText =
        locations.length === 1
          ? 'closest charger location'
          : `closest ${locations.length} charger locations`;
      const filterNotes = [];
      if (highPowerOnly) {
        filterNotes.push('high-power chargers only');
      }
      const filterText = filterNotes.length > 0 ? ` (${filterNotes.join(', ')})` : '';
      setNearMeStatus(`Showing the ${countText} within ${distance} of you${filterText}.`);
    } else {
      setNearMeStatus(
        highPowerOnly
          ? 'No high-power chargers were found near your location.'
          : 'No chargers were found near your location.',
      );
    }
  } catch (error) {
    console.error('Failed to load nearby chargers', error);
    if (error?.code !== undefined) {
      showNearMeError(geolocationErrorMessage(error));
      setNearMeStatus('We need your location to look for nearby chargers.');
    } else {
      showNearMeError('Unable to fetch nearby chargers. Please try again.');
      setNearMeStatus('Fetching nearby chargers failed.');
    }
  } finally {
    setNearMeLoading(false);
    nearMeRequestInFlight = false;
  }
};

const setupNearMePage = () => {
  if (!nearMeSection) {
    return;
  }

  setNearMeStatus('Share your location to find chargers near you.');

  if (!navigator?.geolocation) {
    showNearMeError('Geolocation is not supported by your browser.');
    setNearMeStatus('Location services are unavailable.');
    if (nearMeLocateButton) {
      nearMeLocateButton.disabled = true;
    }
    return;
  }

  if (nearMeLocateButton && !nearMeLocateButton.hasAttribute('data-initialised')) {
    nearMeLocateButton.setAttribute('data-initialised', 'true');
    nearMeLocateButton.addEventListener('click', () => {
      requestNearbyChargers();
    });
  }

  if (
    nearMeIncludeMotorcycleInput &&
    !nearMeIncludeMotorcycleInput.hasAttribute('data-initialised')
  ) {
    nearMeIncludeMotorcycleInput.setAttribute('data-initialised', 'true');
    nearMeIncludeMotorcycleInput.addEventListener('change', () => {
      if (!nearMeRequestInFlight) {
        requestNearbyChargers();
      }
    });
  }

  if (nearMeHighPowerOnlyInput && !nearMeHighPowerOnlyInput.hasAttribute('data-initialised')) {
    nearMeHighPowerOnlyInput.setAttribute('data-initialised', 'true');
    nearMeHighPowerOnlyInput.addEventListener('change', () => {
      if (!nearMeRequestInFlight) {
        requestNearbyChargers();
      }
    });
  }

  if (navigator.permissions?.query) {
    navigator.permissions
      .query({ name: 'geolocation' })
      .then((permissionStatus) => {
        if (permissionStatus.state === 'granted' && !nearMeAutoRequested) {
          nearMeAutoRequested = true;
          requestNearbyChargers();
        } else if (permissionStatus.state === 'denied') {
          setNearMeStatus('Location access is blocked. Update your browser settings to continue.');
        }
        const handlePermissionChange = () => {
          if (permissionStatus.state === 'granted' && !nearMeAutoRequested) {
            nearMeAutoRequested = true;
            requestNearbyChargers();
          }
        };
        if (typeof permissionStatus.addEventListener === 'function') {
          permissionStatus.addEventListener('change', handlePermissionChange);
        } else if ('onchange' in permissionStatus) {
          permissionStatus.onchange = handlePermissionChange;
        }
      })
      .catch(() => {
        // Ignore permission query failures (unsupported browsers).
      });
  }
};

setupNearMePage();

const applyPendingLocationMarker = () => {
  if (!locationMap || !locationMapReady || !locationMapPendingCoords) {
    return;
  }
  if (!isMapLibreAvailable()) {
    return;
  }
  const { lat, lon } = locationMapPendingCoords;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    debugLog('map', 'Discarding pending marker update because coordinates are invalid', locationMapPendingCoords);
    return;
  }
  const maplibregl = window.maplibregl;
  if (locationMapMarker && typeof locationMapMarker.remove === 'function') {
    locationMapMarker.remove();
  }
  if (!locationMapMarkerElement) {
    const markerElement = document.createElement('div');
    markerElement.classList.add('location-map-marker');
    markerElement.setAttribute('role', 'img');
    markerElement.setAttribute('aria-label', 'Charging station');

    const markerDot = document.createElement('span');
    markerDot.classList.add('location-map-marker__dot');
    markerDot.setAttribute('aria-hidden', 'true');

    const markerLabel = document.createElement('span');
    markerLabel.classList.add('location-map-marker__label');
    markerLabel.textContent = 'Charging station';

    markerElement.append(markerLabel, markerDot);
    locationMapMarkerElement = markerElement;
  }
  const debugEnabled = isDebugEnabled('map');
  locationMapMarkerElement.classList.toggle('location-map-marker--debug', debugEnabled);
  const markerLabelEl = locationMapMarkerElement.querySelector('.location-map-marker__label');
  if (markerLabelEl) {
    markerLabelEl.classList.toggle('location-map-marker__label--debug', debugEnabled);
  }
  const markerDotEl = locationMapMarkerElement.querySelector('.location-map-marker__dot');
  if (markerDotEl) {
    markerDotEl.classList.toggle('location-map-marker__dot--debug', debugEnabled);
  }

  locationMapMarker = new maplibregl.Marker({ element: locationMapMarkerElement, anchor: 'bottom' })
    .setLngLat([lon, lat])
    .addTo(locationMap);
  locationMap.jumpTo({ center: [lon, lat], zoom: 16 });
  scheduleLocationMapResize(6);
  locationMapPendingCoords = null;
  if (locationMapNoteEl) {
    locationMapNoteEl.textContent = 'Map data © OpenStreetMap contributors, © CARTO.';
  }
  if (debugEnabled) {
    runInNextFrame(() => {
      if (!locationMapMarkerElement) {
        return;
      }
      const rect = locationMapMarkerElement.getBoundingClientRect();
      const computedStyle = window.getComputedStyle(locationMapMarkerElement);
      debugLog('map', 'Location map marker rendered', {
        lat,
        lon,
        rect: {
          width: rect.width,
          height: rect.height,
          top: rect.top,
          left: rect.left,
        },
        computedStyle: {
          display: computedStyle.display,
          visibility: computedStyle.visibility,
          opacity: computedStyle.opacity,
          zIndex: computedStyle.zIndex,
        },
      });
    });
  }
};

const updateLocationMap = (coords) => {
  if (!locationMapContainer) {
    return;
  }
  const lat = Number(coords?.lat);
  const lon = Number(coords?.lon);
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lon);
  const mapDebugEnabled = isDebugEnabled('map');

  locationMapContainer.classList.toggle('location-map--debug', mapDebugEnabled);
  if (mapDebugEnabled) {
    runInNextFrame(() => {
      const rect = locationMapContainer.getBoundingClientRect();
      const computedStyle = window.getComputedStyle(locationMapContainer);
      debugLog('map', 'Location map container state', {
        hasCoords,
        lat,
        lon,
        rect: {
          width: rect.width,
          height: rect.height,
          top: rect.top,
          left: rect.left,
        },
        computedStyle: {
          display: computedStyle.display,
          visibility: computedStyle.visibility,
          opacity: computedStyle.opacity,
          position: computedStyle.position,
          zIndex: computedStyle.zIndex,
          overflow: computedStyle.overflow,
        },
      });
    });
  }

  if (!hasCoords) {
    debugLog('map', 'Skipping location map initialisation because coordinates are unavailable', coords);
    if (locationMap) {
      if (locationMapResizeHandle) {
        window.removeEventListener('resize', locationMapResizeHandle);
        locationMapResizeHandle = undefined;
      }
      detachLocationMapResizeObserver();
      clearLocationMapResizeTimer();
      locationMapResizeRepeatCount = 0;
      locationMapReady = false;
      if (typeof locationMap.remove === 'function') {
        locationMap.remove();
      }
      locationMap = undefined;
      locationMapMarker = undefined;
      locationMapMarkerElement = undefined;
    }
    locationMapContainer.innerHTML = '';
    if (locationMapNoteEl) {
      locationMapNoteEl.textContent = 'No coordinates available for this location.';
    }
    locationMapPendingCoords = null;
    return;
  }

  if (!isMapLibreAvailable()) {
    debugLog('map', 'MapLibre GL JS is not available, cannot initialise location map');
    detachLocationMapResizeObserver();
    clearLocationMapResizeTimer();
    locationMapResizeRepeatCount = 0;
    if (locationMapNoteEl) {
      locationMapNoteEl.textContent = 'Map unavailable because MapLibre GL JS failed to load.';
    }
    locationMapMarkerElement = undefined;
    return;
  }

  if (!locationMap) {
    const maplibregl = window.maplibregl;
    locationMap = new maplibregl.Map({
      container: locationMapContainer,
      style: MAP_STYLE_URL,
      center: MAP_DEFAULT_CENTER,
      zoom: MAP_DEFAULT_ZOOM,
      attributionControl: false,
      cooperativeGestures: true,
    });
    locationMapReady = false;

    const attribution = new maplibregl.AttributionControl({
      compact: true,
      customAttribution: MAP_ATTRIBUTION,
    });
    locationMap.addControl(attribution, 'bottom-right');

    if (typeof maplibregl.NavigationControl === 'function') {
      locationMap.addControl(new maplibregl.NavigationControl({ visualizePitch: false }), 'top-right');
    }

    locationMap.on('load', () => {
      locationMapReady = true;
      applyPendingLocationMarker();
      scheduleLocationMapResize(2);
    });

    locationMap.on('render', () => {
      if (!locationMapReady && locationMap?.isStyleLoaded?.()) {
        locationMapReady = true;
        applyPendingLocationMarker();
      }
    });

    locationMapResizeHandle = () => {
      scheduleLocationMapResize(3);
    };
    window.addEventListener('resize', locationMapResizeHandle);

    attachLocationMapResizeObserver();
    scheduleLocationMapResize(12);
  }

  locationMapPendingCoords = { lat, lon };
  debugLog('map', 'Queued location map marker update', { lat, lon });
  applyPendingLocationMarker();
};

const populateLocationDetail = (locationId, details) => {
  if (locationTitleEl) {
    locationTitleEl.textContent = `Location ${locationId}`;
  }
  document.title = `Location ${locationId} – Endolla Watcher`;

  if (locationAddressEl) {
    const addressSource =
      typeof details?.address === 'string'
        ? details.address
        : typeof details?.coordinates?.address === 'string'
          ? details.coordinates.address
          : '';
    const address = addressSource.trim();
    if (address) {
      locationAddressEl.textContent = address;
      locationAddressEl.hidden = false;
    } else {
      locationAddressEl.textContent = '';
      locationAddressEl.hidden = true;
    }
  }

  const updatedIso = details?.updated;
  if (locationUpdatedEl) {
    if (updatedIso) {
      const updatedDate = new Date(updatedIso);
      if (!Number.isNaN(updatedDate)) {
        locationUpdatedEl.textContent = `Telemetry refreshed ${updatedDate.toLocaleString()}`;
      } else {
        locationUpdatedEl.textContent = '';
      }
    } else {
      locationUpdatedEl.textContent = '';
    }
  }

  if (locationStationsEl) {
    locationStationsEl.textContent = formatNumber(details?.station_count);
  }
  if (locationPortsEl) {
    locationPortsEl.textContent = formatNumber(details?.port_count);
  }

  const daySummary = details?.summary?.day ?? {};
  const weekSummary = details?.summary?.week ?? {};

  if (locationOccupationDayEl) {
    locationOccupationDayEl.textContent = formatPercent(daySummary.occupation_utilization_pct, 1);
  }
  if (locationActiveDayEl) {
    locationActiveDayEl.textContent = formatPercent(daySummary.active_charging_utilization_pct, 1);
  }
  if (locationAvailabilityEl) {
    locationAvailabilityEl.textContent = formatRatioPercent(weekSummary.availability_ratio, 1);
  }
  if (locationMonitoredEl) {
    locationMonitoredEl.textContent = formatDecimal(weekSummary.monitored_days || 0, {
      minimumFractionDigits: 1,
      maximumFractionDigits: 1,
    });
  }

  locationDayChart = renderLocationUsageChart(
    locationDayChart,
    locationDayChartCanvas,
    locationDayChartStatus,
    details?.usage_day?.timeline,
    'hour',
  );
  locationWeekChart = renderLocationUsageChart(
    locationWeekChart,
    locationWeekChartCanvas,
    locationWeekChartStatus,
    details?.usage_week?.timeline,
    'day',
  );

  updateStationHeatmaps(locationId, details?.stations);
  updateLocationMap(details?.coordinates);
};

const fetchLocationDetails = async (locationId) => {
  setLocationLoading('Fetching the latest telemetry…');
  setLocationError(null);
  if (locationDayChartStatus) {
    locationDayChartStatus.textContent = 'Loading hourly usage…';
    locationDayChartStatus.hidden = false;
  }
  if (locationWeekChartStatus) {
    locationWeekChartStatus.textContent = 'Loading weekly usage…';
    locationWeekChartStatus.hidden = false;
  }

  try {
    const response = await fetch(`${API_BASE}/locations/${encodeURIComponent(locationId)}`);
    if (!response.ok) {
      if (response.status === 404) {
        throw new Error('Location not found.');
      }
      throw new Error(`Backend returned ${response.status}`);
    }
    const payload = await response.json();
    populateLocationDetail(locationId, payload);
    setLocationLoading(null);
  } catch (error) {
    console.error(error);
    if (locationDayChartStatus) {
      locationDayChartStatus.textContent = 'Unable to load hourly usage.';
      locationDayChartStatus.hidden = false;
    }
    if (locationWeekChartStatus) {
      locationWeekChartStatus.textContent = 'Unable to load weekly usage.';
      locationWeekChartStatus.hidden = false;
    }
    setLocationLoading(null);
    setLocationError(error.message || 'Unable to load location details.');
    resetStationHeatmaps('Unable to load station fingerprints.');
    updateLocationMap(null);
  }
};

const initLocationDetailPage = () => {
  if (!locationDetailRoot) {
    return;
  }
  const params = new URLSearchParams(window.location.search);
  const rawId = params.get('id') ?? params.get('location');
  const locationId = rawId ? rawId.trim() : '';
  if (!locationId) {
    setLocationLoading(null);
    setLocationError('No location specified.');
    resetStationHeatmaps('No station data available for this view.');
    return;
  }
  fetchLocationDetails(locationId);
};

const loadDashboard = async (days = DEFAULT_DAYS) => {
  const targetDays = clampDays(days);
  const targetGranularity = determineGranularity(targetDays);
  if (chargesRangeControl) {
    setRangeSelection(String(targetDays));
  }
  if (dashboardController) {
    dashboardController.abort();
  }
  const controller = new AbortController();
  dashboardController = controller;
  setUtilizationData(null, null, null);
  setHeatmapData(null, null);
  try {
    setChartStatus('Loading charging trend…');
    const params = new URLSearchParams({
      days: targetDays.toString(),
      granularity: targetGranularity,
    });
    const response = await fetch(`${API_BASE}/dashboard?${params.toString()}`, {
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(`Backend returned ${response.status}`);
    }
    const data = await response.json();
    if (dashboardController !== controller) {
      return;
    }
    updateSummary(data.stats, data);
    updateUtilization(data.stats?.utilization);
    updateHighChargingLocations(data.stats?.utilization?.locations);
    setHeatmapData(data.stats?.utilization?.locations || [], data.locations);
    updateRules(data.rules, data.rule_counts);
    updateDaily(data.daily);
    const hasSeries = Array.isArray(data.series);
    const series = hasSeries ? data.series : data.daily;
    const responseGranularity = hasSeries
      ? typeof data.series_granularity === 'string'
        ? data.series_granularity
        : targetGranularity
      : 'day';
    const normalizedGranularity =
      typeof responseGranularity === 'string' ? responseGranularity.toLowerCase() : 'day';
    updateChargesChart(series, normalizedGranularity);
    updateProblematic(data.problematic, data.locations);
  } catch (error) {
    if (error.name === 'AbortError') {
      return;
    }
    setChartStatus('Unable to display charging trend.');
    showError(error);
  } finally {
    if (dashboardController === controller) {
      dashboardController = undefined;
    }
  }
};

refreshUtilizationTables();

if (utilizationTabsRoot && utilizationTabButtons.length > 0) {
  const initialTabButton =
    utilizationTabButtons.find((button) => button.classList.contains('is-active')) ?? utilizationTabButtons[0];
  if (initialTabButton && initialTabButton.dataset.tab) {
    setActiveUtilizationTab(initialTabButton.dataset.tab);
  }

  utilizationTabsRoot.addEventListener('click', (event) => {
    const tabButton = event.target.closest('[data-tab]');
    if (!tabButton || !utilizationTabsRoot.contains(tabButton)) {
      return;
    }
    const { tab } = tabButton.dataset;
    if (!tab) {
      return;
    }
    setActiveUtilizationTab(tab);
  });

  utilizationTabsRoot.addEventListener('keydown', (event) => {
    const navigationKeys = ['ArrowLeft', 'ArrowRight', 'Home', 'End'];
    if (!navigationKeys.includes(event.key) || utilizationTabButtons.length === 0) {
      return;
    }
    event.preventDefault();
    let currentIndex = utilizationTabButtons.indexOf(document.activeElement);
    if (currentIndex === -1) {
      currentIndex = utilizationTabButtons.findIndex((button) => button.dataset.tab === activeUtilizationTabId);
      if (currentIndex === -1) {
        currentIndex = 0;
      }
    }
    let nextIndex = currentIndex;
    if (event.key === 'ArrowRight') {
      nextIndex = (currentIndex + 1) % utilizationTabButtons.length;
    } else if (event.key === 'ArrowLeft') {
      nextIndex = (currentIndex - 1 + utilizationTabButtons.length) % utilizationTabButtons.length;
    } else if (event.key === 'Home') {
      nextIndex = 0;
    } else if (event.key === 'End') {
      nextIndex = utilizationTabButtons.length - 1;
    }
    const nextButton = utilizationTabButtons[nextIndex];
    if (nextButton && nextButton.dataset.tab) {
      setActiveUtilizationTab(nextButton.dataset.tab);
      nextButton.focus();
    }
  });
}

if (utilizationSortButton) {
  utilizationSortButton.addEventListener('click', () => {
    utilizationSortDescending = !utilizationSortDescending;
    refreshUtilizationTables();
  });
}

initLocationDetailPage();

if (chargesRangeControl) {
  const focusOption = (option) => {
    if (!option) {
      return;
    }
    option.focus();
  };

  chargesRangeControl.addEventListener('click', (event) => {
    const option = event.target.closest('[data-value]');
    if (!option || !chargesRangeControl.contains(option)) {
      return;
    }
    const { value } = option.dataset;
    if (!value) {
      return;
    }
    if (chargesRangeControl.dataset.selected !== value) {
      setRangeSelection(value);
      loadDashboard(value);
    }
    focusOption(option);
  });

  chargesRangeControl.addEventListener('keydown', (event) => {
    const navigationKeys = ['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'Home', 'End'];
    if (!navigationKeys.includes(event.key)) {
      return;
    }
    event.preventDefault();
    const options = getRangeOptions();
    if (!options.length) {
      return;
    }
    const currentValue = chargesRangeControl.dataset.selected || options[0].dataset.value;
    const currentIndex = options.findIndex((option) => option.dataset.value === currentValue);
    let nextIndex = currentIndex === -1 ? 0 : currentIndex;
    if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') {
      nextIndex = currentIndex <= 0 ? options.length - 1 : currentIndex - 1;
    } else if (event.key === 'ArrowRight' || event.key === 'ArrowDown') {
      nextIndex = currentIndex >= options.length - 1 ? 0 : currentIndex + 1;
    } else if (event.key === 'Home') {
      nextIndex = 0;
    } else if (event.key === 'End') {
      nextIndex = options.length - 1;
    }
    const nextOption = options[nextIndex];
    if (nextOption) {
      const { value } = nextOption.dataset;
      setRangeSelection(value);
      focusOption(nextOption);
      loadDashboard(value);
    }
  });

  setRangeSelection(String(DEFAULT_DAYS));
}

loadDashboard(DEFAULT_DAYS);
