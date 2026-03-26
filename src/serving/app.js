
const API_BASE = window.API_BASE || 'http://localhost:8000';

const WX = {
  Rain:'🌧️',Clouds:'☁️',Clear:'☀️',Thunderstorm:'⛈️',
  Drizzle:'🌦️',Mist:'🌫️',Haze:'🌁',Fog:'🌫️',Snow:'❄️'
};

const fmt = s => s ? s.replace(/_/g,' ') : '—';

function fmtTime(iso) {
  if (!iso) return '—';
  return new Date(iso).toLocaleTimeString('vi-VN',{hour:'2-digit',minute:'2-digit',second:'2-digit',timeZone:'Asia/Ho_Chi_Minh'});
}

function tagHtml(level, label) {
  const cls = ['','tag-green','tag-amber','tag-orange','tag-red'][level] || 'tag-red';
  return `<span class="tag ${cls}">${label ? fmt(label) : '—'}</span>`;
}

function barCls(r) { return !r ? 'bar-red' : r>=.8 ? 'bar-green' : r>=.5 ? 'bar-amber' : 'bar-red'; }

function barHtml(r) {
  const pct = Math.min(100,Math.round((r||0)*100));
  return `<div class="bar-track"><div class="bar-fill ${barCls(r)}" style="width:${pct}%"></div></div>`;
}

setInterval(()=>{
  document.getElementById('clock').textContent =
    new Date().toLocaleString('vi-VN',{timeZone:'Asia/Ho_Chi_Minh',weekday:'short',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'});
},1000);

async function apiFetch(path) {
  const res = await fetch(API_BASE + path);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

function setRefreshing(on) {
  const btn = document.getElementById('btn-refresh');
  btn.disabled = on;
  btn.textContent = on ? '…' : '↻ Làm mới';
}

async function loadHealth() {
  try {
    const j = await apiFetch('/api/health');
    document.getElementById('live-status').textContent   = 'Trực tiếp';
    document.getElementById('total-records').textContent = (+j.total_records).toLocaleString('vi-VN');
    document.getElementById('latest-time').textContent   = fmtTime(j.latest_data);
  } catch { document.getElementById('live-status').textContent = 'Ngoại tuyến'; }
}

async function loadLatest() {
  const tbody = document.getElementById('traffic-tbody');
  try {
    const j    = await apiFetch('/api/traffic/latest');
    const rows = j.data || [];
    if (!rows.length) { tbody.innerHTML='<tr><td colspan="4"><div class="state-msg">Chưa có dữ liệu</div></td></tr>'; return; }
    tbody.innerHTML = rows.map(r=>`
      <tr>
        <td><div class="loc-name">${fmt(r.location_name)}</div></td>
        <td><div class="speed-num">${r.current_speed!=null?r.current_speed.toFixed(1):'—'}<span class="speed-unit">km/h</span></div>${barHtml(r.speed_ratio)}</td>
        <td>${tagHtml(r.congestion_level,r.congestion_label)}</td>
        <td><div class="wx-badge">${WX[r.weather_condition]||'🌡️'} ${r.weather_condition||'—'}</div><div class="wx-temp">${r.temperature!=null?r.temperature.toFixed(1)+'°C':''}</div></td>
      </tr>`).join('');
    document.getElementById('last-update').textContent = 'Cập nhật lúc '+new Date().toLocaleTimeString('vi-VN',{timeZone:'Asia/Ho_Chi_Minh'});
  } catch(e) {
    tbody.innerHTML=`<tr><td colspan="4"><div class="err-box">Không thể kết nối API: ${e.message}</div></td></tr>`;
  }
}

async function loadWeather() {
  const el = document.getElementById('weather-list');
  try {
    const j    = await apiFetch('/api/weather/impact');
    const data = j.data || [];
    if (!data.length) { el.innerHTML='<div class="state-msg">Chưa có dữ liệu</div>'; return; }
    el.innerHTML = data.map(w=>`
      <div class="wx-item">
        <div class="wx-icon">${WX[w.weather_condition]||'🌡️'}</div>
        <div class="wx-info">
          <div class="wx-name">${w.weather_condition}</div>
          <div class="wx-detail">${w.avg_temperature!=null?w.avg_temperature.toFixed(1):'—'}°C · ${w.sample_count} quan sát</div>
        </div>
        <div class="wx-speed">${w.avg_speed!=null?w.avg_speed.toFixed(1):'—'}<small>km/h</small></div>
      </div>`).join('');
  } catch(e) { el.innerHTML=`<div class="err-box">Lỗi: ${e.message}</div>`; }
}

async function loadSummary() {
  const grid = document.getElementById('loc-grid');
  try {
    const j    = await apiFetch('/api/traffic/summary');
    const locs = j.data || [];
    if (locs.length) {
      const valid = locs.filter(l=>l.avg_speed!=null);
      if (valid.length) document.getElementById('avg-speed').textContent = (valid.reduce((s,l)=>s+l.avg_speed,0)/valid.length).toFixed(1);
      document.getElementById('total-vehicles').textContent = locs.reduce((s,l)=>s+(l.total_vehicles||0),0).toLocaleString('vi-VN');
    }
    if (!locs.length) { grid.innerHTML='<div style="background:#fff;padding:36px;grid-column:1/-1"><div class="state-msg">Hệ thống cần tích lũy thêm dữ liệu trong 1 gi�� đầu</div></div>'; return; }
    grid.innerHTML = locs.map(l=>`
      <div class="loc-card" onclick="openDetail('${l.location_name}')">
        <div class="loc-card-line"></div>
        <div class="lc-name">${fmt(l.location_name)}</div>
        <div class="lc-speed">${l.avg_speed!=null?l.avg_speed.toFixed(1):'—'}</div>
        <div class="lc-unit">km/h trung bình</div>
        ${barHtml(l.avg_speed_ratio)}
        <div class="lc-stats">
          <div class="lc-stat"><strong>${(l.total_motorcycle||0).toLocaleString()}</strong>xe máy</div>
          <div class="lc-stat"><strong>${(l.total_car||0).toLocaleString()}</strong>ô tô</div>
          <div class="lc-stat"><strong>${l.total_records}</strong>bản ghi</div>
        </div>
      </div>`).join('');
  } catch(e) { grid.innerHTML=`<div style="background:#fff;padding:20px;grid-column:1/-1"><div class="err-box">Lỗi: ${e.message}</div></div>`; }
}

async function openDetail(name) {
  document.getElementById('modal-title').textContent = fmt(name);
  document.getElementById('modal-body').innerHTML    = '<div class="state-msg"><span class="spinner"></span>Đang tải lịch sử…</div>';
  document.getElementById('modal').classList.add('open');
  try {
    const j    = await apiFetch(`/api/traffic/location/${name}?limit=10`);
    const data = j.data || [];
    if (!data.length) { document.getElementById('modal-body').innerHTML='<div class="state-msg">Chưa có dữ liệu lịch sử</div>'; return; }
    const maxSpd = Math.max(...data.map(d=>d.current_speed||0)) || 1;
    document.getElementById('modal-body').innerHTML = `
      <div style="margin-bottom:14px;font-size:.62rem;color:var(--ink-faint);letter-spacing:.1em;text-transform:uppercase">10 bản ghi gần nhất</div>
      ${data.map(d=>{
        const spd = d.current_speed!=null?d.current_speed.toFixed(1):'—';
        const pct = Math.round(((d.current_speed||0)/maxSpd)*100);
        return `<div class="hist-row">
          <div class="hist-time">${fmtTime(d.event_time)}</div>
          <div class="hist-speed">${spd} <span style="font-size:.58rem;color:var(--ink-faint)">km/h</span></div>
          <div class="hist-bar-wrap">
            <div class="hist-track"><div class="hist-fill ${barCls(d.speed_ratio)}" style="width:${pct}%"></div></div>
            <div class="hist-label">${d.congestion_label?fmt(d.congestion_label):'—'} · ${d.weather_condition||'—'}${d.temperature!=null?' '+d.temperature.toFixed(1)+'°C':''}</div>
          </div>
        </div>`;
      }).join('')}`;
  } catch(e) { document.getElementById('modal-body').innerHTML=`<div class="err-box">Lỗi: ${e.message}</div>`; }
}

function closeModal(e) {
  if (!e || e.target===document.getElementById('modal'))
    document.getElementById('modal').classList.remove('open');
}

document.addEventListener('keydown',e=>{if(e.key==='Escape')closeModal()});

async function loadAll() {
  setRefreshing(true);
  await Promise.all([loadHealth(),loadLatest(),loadWeather(),loadSummary()]);
  setRefreshing(false);
}

loadAll();
setInterval(loadAll,60_000);