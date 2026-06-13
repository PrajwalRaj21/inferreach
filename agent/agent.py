"""
InferReach Monitoring Agent
===========================
Pushes pipeline status to your Supabase database every 60 seconds.
The platform dashboard reads from Supabase and shows it live.

QUICK START (testing on your own machine):
    1. pip install -r requirements.txt
    2. Copy .env.example to .env and fill in values
    3. python agent.py --demo     ← runs with fake data, no Airflow/Kafka needed

FOR A REAL CLIENT:
    1. Copy this folder to their server
    2. Fill in .env with their CLIENT_ID and real Airflow/Kafka details
    3. python agent.py            ← reads from their actual pipelines
"""

import os
import sys
import time
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / '.env')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ── CONFIG ────────────────────────────────────────────────
SUPABASE_URL   = os.getenv('SUPABASE_URL', 'https://bxwpjxxwumhxcbsyzacp.supabase.co')
SUPABASE_KEY   = os.getenv('SUPABASE_KEY')   # anon key or service role key
CLIENT_ID      = os.getenv('CLIENT_ID')      # UUID of this client in your clients table

AIRFLOW_URL    = os.getenv('AIRFLOW_URL')    # e.g. http://localhost:8080  (leave blank if not used)
AIRFLOW_USER   = os.getenv('AIRFLOW_USER', 'admin')
AIRFLOW_PASS   = os.getenv('AIRFLOW_PASS', 'admin')

KAFKA_BROKER   = os.getenv('KAFKA_BROKER')   # e.g. localhost:9092  (leave blank if not used)
POLL_INTERVAL  = int(os.getenv('POLL_INTERVAL', '60'))
DEMO_MODE      = '--demo' in sys.argv        # run with fake data for testing

HEADERS = {
    'apikey':        SUPABASE_KEY or '',
    'Authorization': f'Bearer {SUPABASE_KEY or ""}',
    'Content-Type':  'application/json',
}


# ══════════════════════════════════════════════════════════
# DEMO MODE — fake pipelines for testing without Airflow/Kafka
# ══════════════════════════════════════════════════════════
import random

def get_demo_pipelines():
    """Returns fake pipeline data so you can test the platform without real infra."""
    statuses = ['ok', 'ok', 'ok', 'warning', 'ok']  # mostly ok, one warning
    return [
        {
            'name': 'ecommerce_orders_to_warehouse',
            'source_type': 'kafka',
            'status': 'ok',
            'throughput': random.randint(800000, 900000),
            'lag_ms': random.randint(5, 15),
            'retry_count': 0
        },
        {
            'name': 'events_kafka_stream_processor',
            'source_type': 'kafka',
            'status': 'ok',
            'throughput': random.randint(2000000, 2200000),
            'lag_ms': random.randint(10, 20),
            'retry_count': 0
        },
        {
            'name': 'crm_daily_transform_job',
            'source_type': 'airflow',
            'status': random.choice(['ok', 'warning']),
            'throughput': 0,
            'lag_ms': 0,
            'retry_count': random.randint(0, 2)
        },
        {
            'name': 'ml_feature_pipeline_refresh',
            'source_type': 'dbt',
            'status': 'ok',
            'throughput': random.randint(300000, 350000),
            'lag_ms': random.randint(18, 30),
            'retry_count': 0
        },
        {
            'name': 'raw_clickstream_ingestion',
            'source_type': 'custom',
            'status': random.choice(['ok', 'ok', 'failed']),
            'throughput': 0,
            'lag_ms': 0,
            'retry_count': 0
        },
    ]


# ══════════════════════════════════════════════════════════
# AIRFLOW — reads real DAG statuses
# ══════════════════════════════════════════════════════════
def get_airflow_pipelines():
    if not AIRFLOW_URL:
        return []
    try:
        r = requests.get(
            f'{AIRFLOW_URL}/api/v1/dags',
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=10
        )
        r.raise_for_status()
        results = []
        for dag in r.json().get('dags', []):
            dag_id = dag['dag_id']
            runs = requests.get(
                f'{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date',
                auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=10
            ).json().get('dag_runs', [])
            state = runs[0]['state'] if runs else 'unknown'
            results.append({
                'name': dag_id,
                'source_type': 'airflow',
                'status': {'success':'ok','running':'ok','failed':'failed','queued':'warning','up_for_retry':'warning'}.get(state, 'unknown'),
                'throughput': 0,
                'lag_ms': 0,
                'retry_count': (runs[0].get('try_number', 1) - 1) if runs else 0
            })
        log.info(f'Airflow: {len(results)} DAGs fetched')
        return results
    except Exception as e:
        log.warning(f'Airflow unreachable: {e}')
        return []


# ══════════════════════════════════════════════════════════
# KAFKA — reads real topic list
# ══════════════════════════════════════════════════════════
def get_kafka_pipelines():
    if not KAFKA_BROKER:
        return []
    try:
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, request_timeout_ms=5000)
        topics = [t for t in admin.list_topics() if not t.startswith('__')]
        admin.close()
        log.info(f'Kafka: {len(topics)} topics found')
        return [{
            'name': f'kafka_{topic}',
            'source_type': 'kafka',
            'status': 'ok',
            'throughput': 0,
            'lag_ms': 0,
            'retry_count': 0
        } for topic in topics]
    except Exception as e:
        log.warning(f'Kafka unreachable: {e}')
        return []


# ══════════════════════════════════════════════════════════
# CUSTOM PIPELINES — add your own checks here
# ══════════════════════════════════════════════════════════
def get_custom_pipelines():
    """
    Add your own pipeline checks here.
    Examples are commented out below — uncomment and edit to use.
    """
    pipelines = []

    # ── Check a health endpoint ──────────────────────────
    # try:
    #     r = requests.get('http://your-service/health', timeout=5)
    #     pipelines.append({
    #         'name': 'your_service_name',
    #         'source_type': 'custom',
    #         'status': 'ok' if r.status_code == 200 else 'failed',
    #         'throughput': r.json().get('records_per_min', 0),
    #         'lag_ms': 0,
    #         'retry_count': 0
    #     })
    # except:
    #     pipelines.append({'name': 'your_service_name', 'source_type': 'custom',
    #                       'status': 'failed', 'throughput': 0, 'lag_ms': 0, 'retry_count': 0})

    # ── Check if an output file was updated recently ─────
    # import os as _os
    # path = '/data/output/latest.parquet'
    # if _os.path.exists(path):
    #     age_min = (time.time() - _os.path.getmtime(path)) / 60
    #     pipelines.append({
    #         'name': 'daily_export_job',
    #         'source_type': 'custom',
    #         'status': 'ok' if age_min < 60 else 'warning',
    #         'throughput': 0, 'lag_ms': 0, 'retry_count': 0
    #     })

    return pipelines


# ══════════════════════════════════════════════════════════
# PUSH TO SUPABASE
# ══════════════════════════════════════════════════════════
def push_pipeline(pipeline):
    """Upsert a pipeline row — updates if exists, inserts if not."""
    try:
        r = requests.post(
            f'{SUPABASE_URL}/rest/v1/pipelines',
            headers={**HEADERS, 'Prefer': 'resolution=merge-duplicates,return=minimal'},
            params={'on_conflict': 'client_id,name'},
            json={
                'client_id':   CLIENT_ID,
                'name':        pipeline['name'],
                'source_type': pipeline.get('source_type', 'custom'),
                'status':      pipeline.get('status', 'unknown'),
                'throughput':  pipeline.get('throughput', 0),
                'lag_ms':      pipeline.get('lag_ms', 0),
                'retry_count': pipeline.get('retry_count', 0),
                'last_seen_at': datetime.now(timezone.utc).isoformat()
            },
            timeout=10
        )
        r.raise_for_status()
    except Exception as e:
        log.error(f'Failed to push {pipeline["name"]}: {e}')


def push_metric(pipeline_id, throughput, lag_ms):
    """Store a throughput data point for the chart."""
    try:
        requests.post(
            f'{SUPABASE_URL}/rest/v1/metrics',
            headers=HEADERS,
            json={
                'pipeline_id': pipeline_id,
                'client_id':   CLIENT_ID,
                'throughput':  throughput,
                'lag_ms':      lag_ms
            },
            timeout=10
        ).raise_for_status()
    except Exception as e:
        log.error(f'Failed to push metric: {e}')


def push_alert(severity, title, detail, pipeline_id=None):
    """Create an alert on the platform."""
    try:
        requests.post(
            f'{SUPABASE_URL}/rest/v1/alerts',
            headers=HEADERS,
            json={
                'client_id':   CLIENT_ID,
                'pipeline_id': pipeline_id,
                'severity':    severity,
                'title':       title,
                'detail':      detail
            },
            timeout=10
        ).raise_for_status()
        log.warning(f'Alert pushed: [{severity.upper()}] {title}')
    except Exception as e:
        log.error(f'Failed to push alert: {e}')


def get_pipeline_id(name):
    """Get a pipeline's UUID by name."""
    try:
        r = requests.get(
            f'{SUPABASE_URL}/rest/v1/pipelines',
            headers=HEADERS,
            params={'client_id': f'eq.{CLIENT_ID}', 'name': f'eq.{name}', 'select': 'id'},
            timeout=10
        )
        rows = r.json()
        return rows[0]['id'] if rows else None
    except:
        return None


# ══════════════════════════════════════════════════════════
# ALERT DETECTION — fires when a pipeline changes state
# ══════════════════════════════════════════════════════════
_prev_states = {}

def check_and_alert(pipeline):
    name   = pipeline['name']
    status = pipeline['status']
    prev   = _prev_states.get(name)

    if prev and prev != status:
        pid = get_pipeline_id(name)
        if status == 'failed':
            push_alert('critical', f'{name} failed', f'Status changed from {prev} to failed.', pid)
        elif status == 'warning':
            push_alert('warning', f'{name} degraded', f'Status changed from {prev} to warning.', pid)
        elif status == 'ok' and prev in ('failed', 'warning'):
            push_alert('info', f'{name} recovered', f'Pipeline is back to OK.', pid)

    if pipeline.get('lag_ms', 0) > 2000:
        push_alert('warning', f'High Kafka lag: {name}', f'Consumer lag is {pipeline["lag_ms"]}ms.', get_pipeline_id(name))

    _prev_states[name] = status


# ══════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════
def run():
    if not SUPABASE_KEY:
        log.error('SUPABASE_KEY not set in .env. Exiting.')
        sys.exit(1)
    if not CLIENT_ID:
        log.error('CLIENT_ID not set in .env. Exiting.')
        sys.exit(1)

    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    log.info(f'InferReach Agent starting [{mode}] — client: {CLIENT_ID} — every {POLL_INTERVAL}s')
    if DEMO_MODE:
        log.info('Running in demo mode with simulated data. Use for testing only.')

    while True:
        log.info('--- polling ---')

        if DEMO_MODE:
            pipelines = get_demo_pipelines()
        else:
            pipelines = get_airflow_pipelines() + get_kafka_pipelines() + get_custom_pipelines()

        if not pipelines:
            log.warning('No pipelines found. Check your Airflow/Kafka config or use --demo to test.')
        else:
            for p in pipelines:
                push_pipeline(p)
                check_and_alert(p)
                pid = get_pipeline_id(p['name'])
                if pid and p.get('throughput', 0) > 0:
                    push_metric(pid, p['throughput'], p.get('lag_ms', 0))

        log.info(f'{len(pipelines)} pipelines synced. Next poll in {POLL_INTERVAL}s.')
        time.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    run()