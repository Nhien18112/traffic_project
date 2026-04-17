import { useEffect, useMemo, useState } from 'react';
import {
  LineChart,
  Line,
  ResponsiveContainer,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  Area
} from 'recharts';

const API_BASE = import.meta.env.VITE_API_BASE || '';
const WEATHER_ICON = {
  Rain: 'Rain',
  Clouds: 'Clouds',
  Clear: 'Clear',
  Thunderstorm: 'Storm',
  Drizzle: 'Drizzle',
  Mist: 'Mist',
  Haze: 'Haze',
  Fog: 'Fog',
  Snow: 'Snow'
};

const tabs = [
  { id: 'overview', label: 'Overview' },
  { id: 'traffic', label: 'Traffic' },
  { id: 'weather', label: 'Weather' }
];

const CITY_SCENE_IMAGES = [
  'https://images.unsplash.com/photo-1465447142348-e9952c393450?auto=format&fit=crop&w=1600&q=80',
  'https://images.unsplash.com/photo-1477959858617-67f85cf4f1df?auto=format&fit=crop&w=1600&q=80',
  'https://images.unsplash.com/photo-1469474968028-56623f02e42e?auto=format&fit=crop&w=1600&q=80'
];

const TRAFFIC_STORY_CARDS = [
  {
    title: 'Morning Pulse',
    subtitle: 'Track pre-rush flow in dense corridors',
    image: 'https://images.unsplash.com/photo-1473448912268-2022ce9509d8?auto=format&fit=crop&w=1200&q=80'
  },
  {
    title: 'Rain Stress',
    subtitle: 'Compare weather vs speed drops instantly',
    image: 'https://images.unsplash.com/photo-1519692933481-e162a57d6721?auto=format&fit=crop&w=1200&q=80'
  },
  {
    title: 'Signal Bottlenecks',
    subtitle: 'Locate high-volume, low-speed intersections',
    image: 'https://images.unsplash.com/photo-1461716834815-9a3d6b7f2f12?auto=format&fit=crop&w=1200&q=80'
  }
];

function formatName(value) {
  return value ? value.replaceAll('_', ' ') : '-';
}

function formatTime(value) {
  if (!value) return '-';
  return new Date(value).toLocaleString('vi-VN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
}

function scoreClass(ratio) {
  if (ratio == null) return 'score-low';
  if (ratio >= 0.75) return 'score-high';
  if (ratio >= 0.4) return 'score-mid';
  return 'score-low';
}

function formatRelativeMinutes(value) {
  if (!value) return 'No data';
  const deltaMs = Date.now() - new Date(value).getTime();
  const mins = Math.max(0, Math.round(deltaMs / 60000));
  if (mins < 1) return 'Updated just now';
  return `Updated ${mins} min ago`;
}

async function fetchJson(path, options) {
  const response = await fetch(`${API_BASE}${path}`, options);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

function App() {
  const [activeTab, setActiveTab] = useState('overview');
  const [health, setHealth] = useState(null);
  const [latestRows, setLatestRows] = useState([]);
  const [summaryRows, setSummaryRows] = useState([]);
  const [weatherRows, setWeatherRows] = useState([]);
  const [cameraCoverage, setCameraCoverage] = useState([]);
  const [cameraCritical, setCameraCritical] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [locationFilter, setLocationFilter] = useState('');
  const [minVehicles, setMinVehicles] = useState(0);
  const [sortMode, setSortMode] = useState('speed-asc');
  const [targetSpeed, setTargetSpeed] = useState(35);
  const [selectedLocation, setSelectedLocation] = useState('');
  const [locationHistory, setLocationHistory] = useState([]);
  const [horizonData, setHorizonData] = useState(null);
  const [detailError, setDetailError] = useState('');
  const [loadingDetail, setLoadingDetail] = useState(false);

  const heroImage = CITY_SCENE_IMAGES[new Date().getMinutes() % CITY_SCENE_IMAGES.length];

  const avgSpeed = useMemo(() => {
    const valid = summaryRows.filter((x) => x.avg_speed != null);
    if (!valid.length) return '-';
    const sum = valid.reduce((acc, x) => acc + x.avg_speed, 0);
    return (sum / valid.length).toFixed(1);
  }, [summaryRows]);

  const totalVehicles = useMemo(() => {
    return summaryRows.reduce((acc, x) => acc + (x.total_vehicles || 0), 0).toLocaleString('vi-VN');
  }, [summaryRows]);

  const avgSpeedValue = useMemo(() => {
    const valid = summaryRows.filter((x) => x.avg_speed != null);
    if (!valid.length) return 0;
    const sum = valid.reduce((acc, x) => acc + x.avg_speed, 0);
    return sum / valid.length;
  }, [summaryRows]);

  const filteredSummaryRows = useMemo(() => {
    const keyword = locationFilter.trim().toLowerCase();
    let rows = summaryRows.filter((row) => {
      const name = (row.location_name || '').toLowerCase();
      const vehicles = row.total_vehicles || 0;
      return name.includes(keyword) && vehicles >= minVehicles;
    });

    const sorted = [...rows];
    if (sortMode === 'speed-asc') sorted.sort((a, b) => (a.avg_speed ?? 999) - (b.avg_speed ?? 999));
    if (sortMode === 'speed-desc') sorted.sort((a, b) => (b.avg_speed ?? -1) - (a.avg_speed ?? -1));
    if (sortMode === 'vehicles-desc') sorted.sort((a, b) => (b.total_vehicles || 0) - (a.total_vehicles || 0));
    if (sortMode === 'ratio-asc') sorted.sort((a, b) => (a.avg_speed_ratio ?? 999) - (b.avg_speed_ratio ?? 999));
    return sorted;
  }, [summaryRows, locationFilter, minVehicles, sortMode]);

  const topHotspot = useMemo(() => filteredSummaryRows[0] || null, [filteredSummaryRows]);

  const busiestNode = useMemo(() => {
    if (!summaryRows.length) return null;
    return [...summaryRows].sort((a, b) => (b.total_vehicles || 0) - (a.total_vehicles || 0))[0] || null;
  }, [summaryRows]);

  const weatherRiskCount = useMemo(() => {
    return weatherRows.filter((row) => ['Rain', 'Thunderstorm', 'Drizzle'].includes(row.weather_condition)).length;
  }, [weatherRows]);

  const predictionDrift = useMemo(() => {
    if (!latestRows.length) return 0;
    const valid = latestRows.filter((row) => row.current_speed != null && row.predicted_speed != null);
    if (!valid.length) return 0;
    const driftSum = valid.reduce((acc, row) => acc + Math.abs(row.current_speed - row.predicted_speed), 0);
    return driftSum / valid.length;
  }, [latestRows]);

  const scenarioResult = useMemo(() => {
    if (!avgSpeedValue) {
      return {
        delta: 0,
        etaMinutes: 0,
        status: 'Waiting for summary data'
      };
    }
    const delta = Math.max(0, targetSpeed - avgSpeedValue);
    if (delta <= 0) {
      return {
        delta: 0,
        etaMinutes: 0,
        status: 'Target reached with current performance'
      };
    }
    return {
      delta,
      etaMinutes: Math.ceil(delta * 6),
      status: 'Requires intervention on bottlenecks'
    };
  }, [targetSpeed, avgSpeedValue]);

  const chartSeries = useMemo(() => {
    if (!locationHistory.length) return [];
    return [...locationHistory]
      .reverse()
      .map((item) => ({
        time: formatTime(item.event_time),
        current: item.current_speed,
        predicted: item.predicted_speed
      }));
  }, [locationHistory]);

  const refreshAll = async () => {
    setLoading(true);
    setError('');
    try {
      const [healthData, latestData, summaryData, weatherData, coverageData] = await Promise.all([
        fetchJson('/api/health'),
        fetchJson('/api/traffic/latest'),
        fetchJson('/api/traffic/summary'),
        fetchJson('/api/weather/impact'),
        fetchJson('/api/diagnostics/camera-coverage?hours=1')
      ]);

      setHealth(healthData);
      setLatestRows(latestData.data || []);
      setSummaryRows(summaryData.data || []);
      setWeatherRows(weatherData.data || []);
      setCameraCoverage(coverageData.data || []);
      setCameraCritical(coverageData.critical_locations || []);
    } catch (e) {
      setError(e.message || 'Failed to load dashboard data');
    } finally {
      setLoading(false);
    }
  };

  const openLocationDetail = async (locationName) => {
    if (!locationName) return;
    setSelectedLocation(locationName);
    setLoadingDetail(true);
    setDetailError('');

    try {
      const [historyPayload, horizonPayload] = await Promise.all([
        fetchJson(`/api/traffic/location/${locationName}?limit=20`),
        fetchJson(`/api/traffic/horizon/${locationName}`)
      ]);

      setLocationHistory(historyPayload.data || []);
      setHorizonData(horizonPayload.data || null);
    } catch (e) {
      setLocationHistory([]);
      setHorizonData(null);
      setDetailError(e.message || 'Failed to load location detail');
    } finally {
      setLoadingDetail(false);
    }
  };

  const closeLocationDetail = () => {
    setSelectedLocation('');
    setLocationHistory([]);
    setHorizonData(null);
    setDetailError('');
    setLoadingDetail(false);
  };

  useEffect(() => {
    refreshAll();
    const timer = setInterval(refreshAll, 30000);
    return () => clearInterval(timer);
  }, []);

  return (
    <div className="app-shell">
      <div className="backdrop-grid" />
      <header className="topbar">
        <div>
          <p className="eyebrow">Urban Command Surface</p>
          <h1>Traffic Intelligence Dashboard</h1>
        </div>
        <div className="topbar-actions">
          <div className={`status-pill ${health ? 'online' : 'offline'}`}>
            <span className="dot" />
            {health ? 'System Online' : 'No API'}
          </div>
          <button onClick={refreshAll} disabled={loading}>
            {loading ? 'Refreshing...' : 'Refresh'}
          </button>
        </div>
      </header>

      <section className="hero-stage" style={{ backgroundImage: `url(${heroImage})` }}>
        <div className="hero-overlay" />
        <div className="hero-content">
          <p className="eyebrow">City Live Canvas</p>
          <h2>Realtime Mobility Storyboard</h2>
          <p>
            Theo doi dong giao thong theo thoi gian thuc, loc diem nong linh hoat,
            va mo phong muc toc do muc tieu ngay tren dashboard.
          </p>
          <div className="hero-tags">
            <span>Freshness: {formatRelativeMinutes(health?.latest_data)}</span>
            <span>Prediction Drift: {predictionDrift.toFixed(1)} km/h</span>
            <span>Weather Risk Nodes: {weatherRiskCount}</span>
          </div>
        </div>
      </section>

      <section className="insight-strip">
        <article className="insight-card">
          <h4>Slowest Hotspot</h4>
          <p>{topHotspot ? formatName(topHotspot.location_name) : '-'}</p>
          <small>{topHotspot ? `${topHotspot.avg_speed?.toFixed?.(1) || '-'} km/h` : 'No location'}</small>
        </article>
        <article className="insight-card">
          <h4>Busiest Junction</h4>
          <p>{busiestNode ? formatName(busiestNode.location_name) : '-'}</p>
          <small>{busiestNode ? `${(busiestNode.total_vehicles || 0).toLocaleString('vi-VN')} vehicles` : 'No data'}</small>
        </article>
        <article className="insight-card">
          <h4>System Freshness</h4>
          <p>{formatRelativeMinutes(health?.latest_data)}</p>
          <small>Latest ingest heartbeat</small>
        </article>
      </section>

      <section className="coverage-panel">
        <div className="coverage-head">
          <h3>Camera Coverage (1h)</h3>
          <p className="muted">Theo doi location mat feed camera hoac stale feed.</p>
        </div>
        {cameraCritical.length === 0 ? (
          <div className="coverage-ok">All locations have healthy camera matching in the last hour.</div>
        ) : (
          <div className="coverage-critical-wrap">
            {cameraCritical.map((name) => (
              <span key={name} className="coverage-critical-item">{formatName(name)}</span>
            ))}
          </div>
        )}
        <div className="coverage-grid">
          {cameraCoverage.slice(0, 8).map((row) => (
            <article key={row.location_name} className="coverage-card">
              <h4>{formatName(row.location_name)}</h4>
              <p>Coverage: {row.coverage_pct ?? 0}%</p>
              <p>Zero rows: {row.zero_pct ?? 0}%</p>
              <small>
                {row.stale_minutes == null ? 'No camera data' : `Stale: ${row.stale_minutes} min`}
              </small>
            </article>
          ))}
        </div>
      </section>

      <section className="control-deck">
        <div className="control-item">
          <label htmlFor="filter-location">Filter location</label>
          <input
            id="filter-location"
            value={locationFilter}
            onChange={(e) => setLocationFilter(e.target.value)}
            placeholder="Type location keyword"
          />
        </div>
        <div className="control-item">
          <label htmlFor="filter-vehicles">Min vehicles: {minVehicles}</label>
          <input
            id="filter-vehicles"
            type="range"
            min={0}
            max={250}
            step={5}
            value={minVehicles}
            onChange={(e) => setMinVehicles(Number(e.target.value))}
          />
        </div>
        <div className="control-item">
          <label htmlFor="sort-mode">Sort</label>
          <select id="sort-mode" value={sortMode} onChange={(e) => setSortMode(e.target.value)}>
            <option value="speed-asc">Speed asc (hotspot first)</option>
            <option value="speed-desc">Speed desc</option>
            <option value="vehicles-desc">Vehicles desc</option>
            <option value="ratio-asc">Ratio asc</option>
          </select>
        </div>
      </section>

      <nav className="tab-row">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            className={activeTab === tab.id ? 'active' : ''}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </nav>

      {error && <div className="error-banner">Data load error: {error}</div>}

      {activeTab === 'overview' && (
        <section className="panel-grid">
          <article className="stat-card">
            <h3>Total Records</h3>
            <p>{health?.total_records?.toLocaleString?.('vi-VN') || '-'}</p>
            <small>Latest: {formatTime(health?.latest_data)}</small>
          </article>
          <article className="stat-card">
            <h3>Average Speed</h3>
            <p>{avgSpeed} km/h</p>
            <small>Across active locations</small>
          </article>
          <article className="stat-card">
            <h3>Total Vehicles</h3>
            <p>{totalVehicles}</p>
            <small>Aggregated camera counters</small>
          </article>

          <article className="wide-card">
            <h3>Location Summary</h3>
            <div className="summary-grid">
              {filteredSummaryRows.map((row) => (
                <div
                  className="summary-item summary-clickable"
                  key={row.location_name}
                  onClick={() => openLocationDetail(row.location_name)}
                  role="button"
                  tabIndex={0}
                  onKeyDown={(e) => e.key === 'Enter' && openLocationDetail(row.location_name)}
                >
                  <h4>{formatName(row.location_name)}</h4>
                  <div className="summary-speed">{row.avg_speed?.toFixed?.(1) || '-'} km/h</div>
                  <div className={`score-bar ${scoreClass(row.avg_speed_ratio)}`}>
                    <span style={{ width: `${Math.max(2, Math.round((row.avg_speed_ratio || 0) * 100))}%` }} />
                  </div>
                  <p>{(row.total_vehicles || 0).toLocaleString('vi-VN')} vehicles</p>
                  <div className="vehicle-breakdown">
                    <span>Xe may: {(row.total_motorcycle || 0).toLocaleString('vi-VN')}</span>
                    <span>O to: {(row.total_car || 0).toLocaleString('vi-VN')}</span>
                    <span>Bus/tai: {(row.total_bus_truck || 0).toLocaleString('vi-VN')}</span>
                  </div>
                </div>
              ))}
            </div>
            {filteredSummaryRows.length === 0 && (
              <p className="muted">No locations match current filter.</p>
            )}
          </article>

          <article className="wide-card scenario-card">
            <h3>What-if Simulator</h3>
            <p className="muted">Target average speed to evaluate intervention urgency.</p>
            <div className="scenario-grid">
              <label htmlFor="target-speed" className="scenario-input">
                Target speed: {targetSpeed} km/h
                <input
                  id="target-speed"
                  type="range"
                  min={20}
                  max={55}
                  step={1}
                  value={targetSpeed}
                  onChange={(e) => setTargetSpeed(Number(e.target.value))}
                />
              </label>
              <div className="scenario-result">
                <p>Current avg: {avgSpeed} km/h</p>
                <p>Required uplift: {scenarioResult.delta.toFixed(1)} km/h</p>
                <p>Estimated stabilization: {scenarioResult.etaMinutes} min</p>
                <small>{scenarioResult.status}</small>
              </div>
            </div>
          </article>
        </section>
      )}

      {activeTab === 'traffic' && (
        <section className="table-panel">
          <h3>Realtime Traffic Feed</h3>
          <table>
            <thead>
              <tr>
                <th>Location</th>
                <th>Current</th>
                <th>Predicted</th>
                <th>Congestion</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {latestRows.length === 0 && (
                <tr>
                  <td colSpan={5} className="muted">No traffic feature data yet. Check FAST stream and topic flow.</td>
                </tr>
              )}
              {latestRows.map((row) => (
                <tr key={row.location_name} className="clickable-row" onClick={() => openLocationDetail(row.location_name)}>
                  <td>{formatName(row.location_name)}</td>
                  <td>{row.current_speed?.toFixed?.(1) || '-'} km/h</td>
                  <td>{row.predicted_speed?.toFixed?.(1) || '-'} km/h</td>
                  <td>
                    <span className={`chip ${scoreClass(row.speed_ratio)}`}>
                      {row.congestion_label || 'Unknown'}
                    </span>
                    {row.no_camera_feed && <span className="chip feed-missing">No camera feed</span>}
                  </td>
                  <td>{formatTime(row.event_time)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="muted">Click a row to view model horizons and vehicle-type details.</p>
        </section>
      )}

      {activeTab === 'weather' && (
        <section className="weather-grid">
          {weatherRows.length === 0 && <div className="muted">No weather impact rows available yet.</div>}
          {weatherRows.map((row) => (
            <article key={row.weather_condition} className="weather-card">
              <h4>{WEATHER_ICON[row.weather_condition] || 'Weather'}</h4>
              <p className="wx-title">{row.weather_condition}</p>
              <p>{row.avg_temperature?.toFixed?.(1) || '-'} C</p>
              <p>{row.avg_speed?.toFixed?.(1) || '-'} km/h</p>
              <small>{row.sample_count} samples</small>
            </article>
          ))}
        </section>
      )}

      <section className="story-wall">
        {TRAFFIC_STORY_CARDS.map((card) => (
          <article className="story-card" key={card.title}>
            <img src={card.image} alt={card.title} loading="lazy" />
            <div className="story-copy">
              <h4>{card.title}</h4>
              <p>{card.subtitle}</p>
            </div>
          </article>
        ))}
      </section>

      {selectedLocation && (
        <div className="detail-overlay" onClick={closeLocationDetail}>
          <div className="detail-modal" onClick={(e) => e.stopPropagation()}>
            <div className="detail-header">
              <div>
                <h3>{formatName(selectedLocation)}</h3>
                <p className="muted">Model and vehicle detail</p>
              </div>
              <button onClick={closeLocationDetail}>Close</button>
            </div>

            {loadingDetail && <p className="muted">Loading detail...</p>}
            {!loadingDetail && detailError && <div className="error-banner">{detailError}</div>}

            {!loadingDetail && !detailError && (
              <>
                <section className="detail-chart-card">
                  <h4>Speed Timeline (Current vs AI Predict)</h4>
                  <p className="muted">Khung quan sát gần nhất cho location đang chọn.</p>
                  <div className="chart-wrap">
                    <ResponsiveContainer width="100%" height={280}>
                      <LineChart data={chartSeries} margin={{ top: 12, right: 18, left: 0, bottom: 0 }}>
                        <defs>
                          <linearGradient id="currentFill" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="0%" stopColor="#00c9a7" stopOpacity={0.28} />
                            <stop offset="100%" stopColor="#00c9a7" stopOpacity={0.03} />
                          </linearGradient>
                        </defs>
                        <CartesianGrid stroke="rgba(148,163,184,0.15)" vertical={false} />
                        <XAxis dataKey="time" tick={{ fill: '#cbd5e1', fontSize: 12 }} axisLine={false} tickLine={false} minTickGap={28} />
                        <YAxis tick={{ fill: '#cbd5e1', fontSize: 12 }} axisLine={false} tickLine={false} width={42} />
                        <Tooltip
                          contentStyle={{ background: '#0f172a', border: '1px solid rgba(148,163,184,0.35)', borderRadius: 10, color: '#e2e8f0' }}
                          labelStyle={{ color: '#e2e8f0' }}
                        />
                        <Legend wrapperStyle={{ color: '#cbd5e1', fontSize: 12 }} />
                        <Area type="monotone" dataKey="current" stroke="none" fill="url(#currentFill)" />
                        <Line
                          type="monotone"
                          dataKey="current"
                          name="Toc do Hien tai"
                          stroke="#00c9a7"
                          strokeWidth={2.2}
                          dot={false}
                          activeDot={{ r: 4 }}
                        />
                        <Line
                          type="monotone"
                          dataKey="predicted"
                          name="AI Predict"
                          stroke="#8b5cf6"
                          strokeWidth={2}
                          strokeDasharray="6 4"
                          dot={{ r: 3 }}
                          connectNulls
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </section>

                <section className="detail-grid">
                  <article className="detail-card">
                    <h4>Model</h4>
                    <p>Version: {horizonData?.model_version || 'N/A'}</p>
                    <p>Current: {horizonData?.current_speed?.toFixed?.(1) || '-'} km/h</p>
                    <p>Baseline: {horizonData?.free_flow_speed?.toFixed?.(1) || '-'} km/h</p>
                    <p>Now label: {horizonData?.predicted_congestion_label || 'N/A'}</p>
                  </article>

                  <article className="detail-card">
                    <h4>Prediction Horizons</h4>
                    <p>+5m: {horizonData?.horizons?.['5m']?.speed?.toFixed?.(1) || '-'} km/h ({horizonData?.horizons?.['5m']?.label || 'N/A'})</p>
                    <p>+10m: {horizonData?.horizons?.['10m']?.speed?.toFixed?.(1) || '-'} km/h ({horizonData?.horizons?.['10m']?.label || 'N/A'})</p>
                    <p>+15m: {horizonData?.horizons?.['15m']?.speed?.toFixed?.(1) || '-'} km/h ({horizonData?.horizons?.['15m']?.label || 'N/A'})</p>
                  </article>

                  <article className="detail-card">
                    <h4>Vehicle Types (latest)</h4>
                    {locationHistory[0]?.no_camera_feed && (
                      <p className="warning-text">Camera feed missing near this timestamp.</p>
                    )}
                    <p>Xe may: {(locationHistory[0]?.motorcycle_count || 0).toLocaleString('vi-VN')}</p>
                    <p>O to: {(locationHistory[0]?.car_count || 0).toLocaleString('vi-VN')}</p>
                    <p>Bus/tai: {(locationHistory[0]?.bus_truck_count || 0).toLocaleString('vi-VN')}</p>
                  </article>
                </section>

                <section className="detail-table-wrap">
                  <h4>Recent Timeline</h4>
                  <table>
                    <thead>
                      <tr>
                        <th>Time</th>
                        <th>Current</th>
                        <th>Pred</th>
                        <th>Xe may</th>
                        <th>O to</th>
                        <th>Bus/tai</th>
                      </tr>
                    </thead>
                    <tbody>
                      {locationHistory.map((item) => (
                        <tr key={`${selectedLocation}-${item.event_time}`}>
                          <td>{formatTime(item.event_time)}</td>
                          <td>{item.current_speed?.toFixed?.(1) || '-'}</td>
                          <td>{item.predicted_speed?.toFixed?.(1) || '-'}</td>
                          <td>{(item.motorcycle_count || 0).toLocaleString('vi-VN')}</td>
                          <td>{(item.car_count || 0).toLocaleString('vi-VN')}</td>
                          <td>{(item.bus_truck_count || 0).toLocaleString('vi-VN')}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </section>
              </>
            )}
          </div>
        </div>
      )}

    </div>
  );
}

export default App;
