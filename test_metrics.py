import json
from fastapi.testclient import TestClient
from app import app
from services.config import DASHBOARD_API_KEY

headers = {
    "X-API-Key": DASHBOARD_API_KEY
}

with TestClient(app) as client:
    for svc in ['openclaw', 'hermes', '9router']:
        # Request with X-API-Key header to authenticate
        r = client.get(f'/api/services/{svc}/metrics', headers=headers)
        body = r.json()
        print(f'{svc}: HTTP {r.status_code}')
        if r.status_code == 200:
            print(f'  pid={body.get("pid")}, rt={body.get("response_time_ms")}ms, err5m={body.get("error_count_5min")}, warns5m={body.get("warning_count_5min")}, restarts24h={body.get("restart_count_24h")}, mem={body.get("memory_mb")}MB, children={body.get("child_process_count")}, uptime={body.get("uptime")}')
        else:
            print(f'  Error: {body}')
