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

let chargesChart;
let dashboardController;

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
  const meta = document.getElementById('summary-meta');
  if (meta) {
    meta.textContent = 'Backend unavailable';
  }
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
