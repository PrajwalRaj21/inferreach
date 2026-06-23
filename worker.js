const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PATCH, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: { ...CORS, 'Content-Type': 'application/json' } });
}

function getToken(request) {
  const auth = request.headers.get('Authorization') || '';
  return auth.replace('Bearer ', '').trim();
}

const API_PATHS = ['/auth', '/pipelines', '/alerts', '/metrics'];

export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS });

    const url = new URL(request.url);
    const path = url.pathname;

    // ── SERVE STATIC ASSETS for non-API routes ──
    if (!API_PATHS.some(p => path.startsWith(p))) {
      return json({error: 'Not found'}, 404);
    }

    // ── AUTH ──
    if (path === '/auth/login' && request.method === 'POST') {
      const { email, password } = await request.json();

      const client = await env.DB.prepare('SELECT * FROM clients WHERE email = ?').bind(email).first();
      if (!client) return json({ error: 'Invalid credentials' }, 401);

      if (client.password_hash && client.password_hash !== password) {
        return json({ error: 'Invalid credentials' }, 401);
      }

      const token = btoa(`${email}:${Date.now()}`);
      return json({ token, user: { email, full_name: client.name }, client });
    }

    // ── PROTECTED ROUTES — verify token exists ──
    const token = getToken(request);
    if (!token) return json({ error: 'Unauthorized' }, 401);

    // ── PIPELINES ──
    if (path === '/pipelines') {
      if (request.method === 'GET') {
        const { results } = await env.DB.prepare('SELECT * FROM pipelines ORDER BY status ASC').all();
        return json(results);
      }
      if (request.method === 'POST') {
        const p = await request.json();
        await env.DB.prepare(
          'INSERT INTO pipelines (id,client_id,name,description,source_type,status,throughput,lag_ms,retry_count,last_seen_at) VALUES (?,?,?,?,?,?,?,?,?,?)'
        ).bind(p.id||crypto.randomUUID(),p.client_id,p.name,p.description||'',p.source_type||'custom',p.status||'unknown',p.throughput||0,p.lag_ms||0,p.retry_count||0,p.last_seen_at||null).run();
        return json({ ok: true });
      }
      if (request.method === 'PATCH') {
        const p = await request.json();
        await env.DB.prepare(
          'UPDATE pipelines SET status=?,throughput=?,lag_ms=?,retry_count=?,last_seen_at=? WHERE id=?'
        ).bind(p.status,p.throughput,p.lag_ms,p.retry_count,p.last_seen_at,p.id).run();
        return json({ ok: true });
      }
    }

    // ── ALERTS ──
    if (path === '/alerts') {
      if (request.method === 'GET') {
        const { results } = await env.DB.prepare('SELECT * FROM alerts ORDER BY created_at DESC').all();
        return json(results);
      }
      if (request.method === 'POST') {
        const a = await request.json();
        await env.DB.prepare(
          'INSERT INTO alerts (id,client_id,pipeline_id,severity,title,detail) VALUES (?,?,?,?,?,?)'
        ).bind(crypto.randomUUID(),a.client_id,a.pipeline_id||null,a.severity,a.title,a.detail||'').run();
        return json({ ok: true });
      }
    }

    // ── METRICS ──
    if (path === '/metrics' && request.method === 'POST') {
      const m = await request.json();
      await env.DB.prepare(
        'INSERT INTO metrics (id,pipeline_id,client_id,throughput,lag_ms) VALUES (?,?,?,?,?)'
      ).bind(crypto.randomUUID(),m.pipeline_id,m.client_id,m.throughput,m.lag_ms||0).run();
      return json({ ok: true });
    }

    return json({ error: 'Not found' }, 404);
  }
};