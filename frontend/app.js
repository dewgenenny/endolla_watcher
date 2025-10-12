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
let locationMapResizeHandle;
let locationMapResizeObserver;
let locationMapResizeTimer;
let locationMapResizeTimerType;
let locationMapResizeRepeatCount = 0;

const UTILIZATION_VIEW_IDS = ['locations', 'stations', 'ports'];
const UTILIZATION_PAGE_SIZES = [10, 25, 100];
const UTILIZATION_DEFAULT_PAGE_SIZE = UTILIZATION_PAGE_SIZES[0];

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

const formatMinutes = (value) => {
  if (!value && value !== 0) {
    return '–';
  }
  return `${value.toFixed(1)} min`;
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
    locationMap.invalidateSize();
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

const updateLocationMap = (coords) => {
  if (!locationMapContainer) {
    return;
  }
  const lat = Number(coords?.lat);
  const lon = Number(coords?.lon);
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lon);

  if (!hasCoords) {
    if (locationMap) {
      if (locationMapResizeHandle) {
        window.removeEventListener('resize', locationMapResizeHandle);
        locationMapResizeHandle = undefined;
      }
      detachLocationMapResizeObserver();
      clearLocationMapResizeTimer();
      locationMapResizeRepeatCount = 0;
      locationMap.remove();
      locationMap = undefined;
      locationMapMarker = undefined;
    }
    locationMapContainer.innerHTML = '';
    if (locationMapNoteEl) {
      locationMapNoteEl.textContent = 'No coordinates available for this location.';
    }
    return;
  }

  if (typeof window.L === 'undefined') {
    detachLocationMapResizeObserver();
    clearLocationMapResizeTimer();
    locationMapResizeRepeatCount = 0;
    if (locationMapNoteEl) {
      locationMapNoteEl.textContent = 'Map library failed to load.';
    }
    return;
  }

  if (!locationMap) {
    locationMap = window.L.map(locationMapContainer, { attributionControl: false });
    window.L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 19,
      attribution: '© OpenStreetMap contributors',
    }).addTo(locationMap);

    locationMapResizeHandle = () => {
      scheduleLocationMapResize(3);
    };
    window.addEventListener('resize', locationMapResizeHandle);

    locationMap.whenReady(() => {
      scheduleLocationMapResize(3);
    });

    attachLocationMapResizeObserver();
    scheduleLocationMapResize(12);

    locationMap.on('load', () => {
      scheduleLocationMapResize(2);
    });
  }

  if (locationMapMarker) {
    locationMap.removeLayer(locationMapMarker);
  }
  locationMapMarker = window.L.marker([lat, lon]).addTo(locationMap);
  locationMap.setView([lat, lon], 16);
  scheduleLocationMapResize(6);

  if (locationMapNoteEl) {
    locationMapNoteEl.textContent = 'Map data © OpenStreetMap contributors.';
  }
};

const populateLocationDetail = (locationId, details) => {
  if (locationTitleEl) {
    locationTitleEl.textContent = `Location ${locationId}`;
  }
  document.title = `Location ${locationId} – Endolla Watcher`;

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
