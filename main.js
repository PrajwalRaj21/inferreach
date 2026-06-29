/* ============================================================
   InferReach — main.js
   ============================================================ */

/* ---- Typewriter ---- */
function initTypewriter() {
  const el = document.getElementById('typewriter');
  if (!el) return;
  const lines = [
    'deploy --pipeline ecommerce_to_warehouse --env prod',
    'ingest --source kafka --topic orders --sink bigquery',
    'dbt run --models staging.orders --target prod',
    'airflow dags trigger ml_feature_refresh',
    'monitor --pipeline clickstream --alert p99_latency',
  ];
  let lineIdx = 0, charIdx = 0, deleting = false;
  function tick() {
    const current = lines[lineIdx];
    if (!deleting) {
      el.textContent = current.slice(0, charIdx + 1);
      charIdx++;
      if (charIdx === current.length) { deleting = true; setTimeout(tick, 2200); return; }
    } else {
      el.textContent = current.slice(0, charIdx - 1);
      charIdx--;
      if (charIdx === 0) { deleting = false; lineIdx = (lineIdx + 1) % lines.length; }
    }
    setTimeout(tick, deleting ? 28 : 52);
  }
  tick();
}

/* ---- Dashboard sidebar responsive ---- */
function fixDashGrid() {
  const body = document.getElementById('dashBody');
  if (!body) return;
  const sidebar = body.querySelector('.dash-sidebar');
  if (!sidebar) return;
  const hidden = window.getComputedStyle(sidebar).display === 'none';
  body.style.gridTemplateColumns = hidden ? '1fr' : '172px 1fr';
}

/* ---- Scroll animations ---- */
function initScrollAnimations() {
  const els = document.querySelectorAll('.service-card, .process-step, .stack-layer');
  const observer = new IntersectionObserver((entries) => {
    entries.forEach((entry, i) => {
      if (entry.isIntersecting) {
        entry.target.style.animation = `fadeInUp 0.45s ${i * 0.06}s ease both`;
        observer.unobserve(entry.target);
      }
    });
  }, { threshold: 0.08 });
  els.forEach(el => { el.style.opacity = '0'; observer.observe(el); });
}

/* ---- Pipeline canvas ---- */
(function () {
  const canvas = document.getElementById('pipelineCanvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');

  function resize() {
    const rect = canvas.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    if (!rect.width) return;
    canvas.width  = rect.width  * dpr;
    canvas.height = rect.height * dpr;
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.scale(dpr, dpr);
  }
  requestAnimationFrame(resize);
  window.addEventListener('resize', resize);

  const W = () => canvas.offsetWidth;
  const H = () => canvas.offsetHeight;

  const COLORS = {
    green: '#10b981', yellow: '#ffd166', blue: '#00b4d8',
    orange: '#ff6b35', red: '#ff4757', dim: '#1a2a3a',
    grid: 'rgba(16,185,129,0.03)', text: '#445566'
  };

  const getNodes = () => [
    { id: 'db',     x: 0.08, y: 0.2,  label: 'Postgres',     color: COLORS.blue   },
    { id: 'api',    x: 0.08, y: 0.5,  label: 'REST API',     color: COLORS.blue   },
    { id: 'events', x: 0.08, y: 0.8,  label: 'Events',       color: COLORS.blue   },
    { id: 'kafka',  x: 0.32, y: 0.5,  label: 'Kafka',        color: COLORS.orange },
    { id: 'spark',  x: 0.56, y: 0.28, label: 'Spark',        color: COLORS.yellow },
    { id: 'flink',  x: 0.56, y: 0.72, label: 'Flink',        color: COLORS.yellow },
    { id: 'snow',   x: 0.82, y: 0.2,  label: 'Snowflake',    color: COLORS.green  },
    { id: 'bq',     x: 0.82, y: 0.5,  label: 'BigQuery',     color: COLORS.green  },
    { id: 'ml',     x: 0.82, y: 0.8,  label: 'Feature Store',color: COLORS.green  },
  ];

  const EDGES = [
    { from: 'db', to: 'kafka' }, { from: 'api', to: 'kafka' }, { from: 'events', to: 'kafka' },
    { from: 'kafka', to: 'spark' }, { from: 'kafka', to: 'flink' },
    { from: 'spark', to: 'snow' }, { from: 'spark', to: 'bq' },
    { from: 'flink', to: 'bq' }, { from: 'flink', to: 'ml' },
  ];

  const particles = [];
  function spawnParticle() {
    const edge = EDGES[Math.floor(Math.random() * EDGES.length)];
    particles.push({
      edge, t: 0,
      speed: 0.004 + Math.random() * 0.006,
      size:  2 + Math.random() * 2,
      color: Math.random() > 0.6 ? COLORS.green : Math.random() > 0.5 ? COLORS.yellow : COLORS.blue
    });
  }
  for (let i = 0; i < 18; i++) {
    particles.push({
      edge:  EDGES[Math.floor(Math.random() * EDGES.length)],
      t:     Math.random(),
      speed: 0.004 + Math.random() * 0.006,
      size:  2 + Math.random() * 2,
      color: Math.random() > 0.6 ? COLORS.green : Math.random() > 0.5 ? COLORS.yellow : COLORS.blue
    });
  }

  let frame = 0;
  function draw() {
    const w = W(), h = H();
    ctx.clearRect(0, 0, w, h);

    // grid
    ctx.strokeStyle = COLORS.grid; ctx.lineWidth = 1;
    for (let x = 0; x < w; x += 32) { ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, h); ctx.stroke(); }
    for (let y = 0; y < h; y += 32) { ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke(); }

    const nodes = getNodes();
    const nodeMap = {};
    nodes.forEach(n => { nodeMap[n.id] = { ...n, px: n.x * w, py: n.y * h }; });

    // edges
    EDGES.forEach(edge => {
      const a = nodeMap[edge.from], b = nodeMap[edge.to];
      const mx = (a.px + b.px) / 2;
      ctx.beginPath(); ctx.moveTo(a.px, a.py);
      ctx.bezierCurveTo(mx, a.py, mx, b.py, b.px, b.py);
      ctx.strokeStyle = 'rgba(16,185,129,0.10)'; ctx.lineWidth = 1.5; ctx.stroke();
    });

    // particles
    if (frame % 8 === 0 && particles.length < 40) spawnParticle();
    for (let i = particles.length - 1; i >= 0; i--) {
      const p = particles[i]; p.t += p.speed;
      if (p.t > 1) { particles.splice(i, 1); continue; }
      const a = nodeMap[p.edge.from], b = nodeMap[p.edge.to];
      const mx = (a.px + b.px) / 2, t = p.t, mt = 1 - t;
      const px = mt*mt*mt*a.px + 3*mt*mt*t*mx + 3*mt*t*t*mx + t*t*t*b.px;
      const py = mt*mt*mt*a.py + 3*mt*mt*t*a.py + 3*mt*t*t*b.py + t*t*t*b.py;
      ctx.beginPath(); ctx.arc(px, py, p.size, 0, Math.PI * 2);
      ctx.fillStyle = p.color; ctx.shadowColor = p.color; ctx.shadowBlur = 4; ctx.fill(); ctx.shadowBlur = 0;
    }

    // nodes
    nodes.forEach(n => {
      const node = nodeMap[n.id];
      const pulse = 1 + Math.sin(frame * 0.05 + n.x * 10) * 0.07;
      ctx.beginPath(); ctx.arc(node.px, node.py, 15 * pulse, 0, Math.PI * 2);
      ctx.fillStyle = 'rgba(13,17,23,0.95)'; ctx.strokeStyle = n.color; ctx.lineWidth = 1.5; ctx.fill(); ctx.stroke();
      ctx.beginPath(); ctx.arc(node.px, node.py, 19 * pulse, 0, Math.PI * 2);
      const grad = ctx.createRadialGradient(node.px, node.py, 7, node.px, node.py, 22);
      grad.addColorStop(0, n.color + '22'); grad.addColorStop(1, 'transparent');
      ctx.fillStyle = grad; ctx.fill();
      ctx.font = `500 9px 'IBM Plex Mono', monospace`;
      ctx.fillStyle = n.color; ctx.textAlign = 'center';
      ctx.fillText(n.label, node.px, node.py + 28);
    });

    frame++;
    requestAnimationFrame(draw);
  }
  draw();
})();

/* ---- Throughput chart ---- */
(function () {
  const canvas = document.getElementById('throughputChart');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');

  function resize() {
    const rect = canvas.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    if (!rect.width) return;
    canvas.width  = rect.width  * dpr;
    canvas.height = rect.height * dpr;
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.scale(dpr, dpr);
  }
  function initChart() {
    const rect = canvas.getBoundingClientRect();
    if (rect.width > 0) { resize(); drawChart(); return; }
    let tries = 0;
    const poll = setInterval(() => {
      const r = canvas.getBoundingClientRect();
      if (r.width > 0 || ++tries > 20) { clearInterval(poll); resize(); drawChart(); }
    }, 100);
  }
  if ('ResizeObserver' in window) {
    const ro = new ResizeObserver(() => { resize(); drawChart(); });
    ro.observe(canvas);
  }
  window.addEventListener('resize', () => { resize(); drawChart(); });
  setTimeout(initChart, 50);

  const points = 96;
  const data = [];
  let base = 1800000;
  for (let i = 0; i < points; i++) {
    const hour   = i / 4;
    const daily  = Math.sin((hour - 6) * Math.PI / 12) * 800000;
    const noise  = (Math.random() - 0.5) * 300000;
    const spike  = (i === 52 || i === 53) ? 600000 : 0;
    data.push(Math.max(200000, base + daily + noise + spike));
  }

  function drawChart() {
    const w = canvas.offsetWidth, h = canvas.offsetHeight;
    ctx.clearRect(0, 0, w, h);
    const pad = { top: 10, right: 10, bottom: 24, left: 10 };
    const chartW = w - pad.left - pad.right, chartH = h - pad.top - pad.bottom;
    const maxVal = Math.max(...data) * 1.1, minVal = 0;

    // grid lines
    for (let i = 0; i <= 4; i++) {
      const y = pad.top + chartH - (chartH * i / 4);
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + chartW, y);
      ctx.strokeStyle = 'rgba(16,185,129,0.04)'; ctx.lineWidth = 1; ctx.stroke();
      const val = Math.round((maxVal * i / 4) / 1000);
      ctx.font = '9px IBM Plex Mono, monospace'; ctx.fillStyle = '#445566'; ctx.textAlign = 'right';
      ctx.fillText(val + 'K', pad.left + 36, y + 3);
    }

    // area fill
    ctx.beginPath();
    data.forEach((v, i) => {
      const x = pad.left + (i / (points - 1)) * chartW;
      const y = pad.top + chartH - ((v - minVal) / (maxVal - minVal)) * chartH;
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.lineTo(pad.left + chartW, pad.top + chartH);
    ctx.lineTo(pad.left, pad.top + chartH);
    ctx.closePath();
    const areaGrad = ctx.createLinearGradient(0, pad.top, 0, pad.top + chartH);
    areaGrad.addColorStop(0, 'rgba(16,185,129,0.14)');
    areaGrad.addColorStop(1, 'rgba(16,185,129,0.01)');
    ctx.fillStyle = areaGrad; ctx.fill();

    // line
    ctx.beginPath();
    data.forEach((v, i) => {
      const x = pad.left + (i / (points - 1)) * chartW;
      const y = pad.top + chartH - ((v - minVal) / (maxVal - minVal)) * chartH;
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = '#10b981'; ctx.lineWidth = 1.5;
    ctx.shadowColor = '#10b981'; ctx.shadowBlur = 3; ctx.stroke(); ctx.shadowBlur = 0;

    // spike marker
    const spikeIdx = 52;
    const sx = pad.left + (spikeIdx / (points - 1)) * chartW;
    const sy = pad.top + chartH - ((data[spikeIdx] - minVal) / (maxVal - minVal)) * chartH;
    ctx.beginPath(); ctx.arc(sx, sy, 3.5, 0, Math.PI * 2);
    ctx.fillStyle = '#ff6b35'; ctx.shadowColor = '#ff6b35'; ctx.shadowBlur = 5; ctx.fill(); ctx.shadowBlur = 0;

    // x labels
    ['00:00','06:00','12:00','18:00','24:00'].forEach((lbl, i) => {
      const x = pad.left + (i / 4) * chartW;
      ctx.font = '9px IBM Plex Mono, monospace'; ctx.fillStyle = '#445566'; ctx.textAlign = 'center';
      ctx.fillText(lbl, x, h - 4);
    });
  }
})();

/* ---- Page loader ---- */
(function () {
  const loader  = document.getElementById('loader');
  if (!loader) return;
  document.body.style.overflow = 'hidden';
  const status    = document.getElementById('loaderStatus');
  const nodes     = [document.getElementById('ln1'), document.getElementById('ln2'), document.getElementById('ln3')];
  const particles = [document.getElementById('lpar1'), document.getElementById('lpar2')];

  const steps = [
    { msg: 'connecting to data sources...', node: 0 },
    { msg: 'running ingestion pipeline...',  node: 0, particle: 0 },
    { msg: 'applying transformations...',    node: 1, particle: 1 },
    { msg: 'loading to warehouse...',        node: 2 },
    { msg: 'pipeline ready.',                node: 2 },
  ];

  let step = 0;
  nodes[0].classList.add('active');

  function animateParticle(p, cb) {
    let pos = 0; p.style.opacity = '1';
    const run = () => {
      pos += 3; p.style.left = pos + 'px';
      if (pos < 90) requestAnimationFrame(run);
      else { p.style.opacity = '0'; p.style.left = '-8px'; cb && cb(); }
    };
    requestAnimationFrame(run);
  }

  function runStep() {
    if (step >= steps.length) {
      setTimeout(() => {
        loader.classList.add('hidden');
        setTimeout(() => { loader.remove(); document.body.style.overflow = ''; }, 600);
      }, 400);
      return;
    }
    const s = steps[step];
    if (status) status.textContent = s.msg;
    if (s.node !== undefined && nodes[s.node]) nodes[s.node].classList.add('active');
    if (s.particle !== undefined) {
      animateParticle(particles[s.particle], () => { step++; setTimeout(runStep, 300); });
    } else {
      step++;
      setTimeout(runStep, step === steps.length ? 200 : 500);
    }
  }
  setTimeout(runStep, 300);
})();

/* ---- Theme toggle ---- */
(function () {
  const btn = document.getElementById('themeToggle');
  if (!btn) return;
  const saved = localStorage.getItem('theme');
  if (saved === 'light') document.body.classList.add('light');
  btn.addEventListener('click', () => {
    document.body.classList.toggle('light');
    localStorage.setItem('theme', document.body.classList.contains('light') ? 'light' : 'dark');
  });
})();

/* ---- Init on DOM ready ---- */
document.addEventListener('DOMContentLoaded', () => {
  initTypewriter();
  initScrollAnimations();
  fixDashGrid();
  window.addEventListener('resize', fixDashGrid);
});
