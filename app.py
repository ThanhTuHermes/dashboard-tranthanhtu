import asyncio
import json
import os
import re
import subprocess
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path

import psutil
import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Dashboard Monitor - TranThanhTu.site")

if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

DASHBOARD_HTML_PATH = Path(__file__).parent / "templates" / "dashboard.html"
DASHBOARD_HTML = DASHBOARD_HTML_PATH.read_text(encoding="utf-8") if DASHBOARD_HTML_PATH.exists() else "<h1>Dashboard Template Not Found</h1>"

VN_TZ = timezone(timedelta(hours=7))

MAX_HISTORY = 60
history = deque(maxlen=MAX_HISTORY)

SERVICES = {
    "openclaw": {
        "name": "OpenClaw Gateway", "port": 18789, "systemd": None,
        "description": "AI Agent Gateway", "monitoring_depth": "full",
    },
    "hermes": {
        "name": "Hermes Agent", "port": 9119, "systemd": "hermes.service",
        "description": "Hermes AI Dashboard & LSP", "monitoring_depth": "full",
    },
    "9router": {
        "name": "9Router AI Gateway", "port": 20128, "systemd": "9router.service",
        "description": "AI Model Router", "monitoring_depth": "full",
    },
}

LOG_SOURCES = {
    "system": None,
    "openclaw": None,
    "hermes": "hermes.service",
    "9router": "9router.service",
    "dashboard": "dashboard.service",
    "cloudflared": "cloudflared.service",
}

THRESHOLDS = {
    "cpu": {"warn": 60, "crit": 80},
    "memory": {"warn": 70, "crit": 85},
    "disk": {"warn": 75, "crit": 90},
}


class ServiceMonitor:
    """Per-service deep monitoring using non-blocking methods."""

    def __init__(self, key: str, port: int, systemd_unit: str | None):
        self.key = key
        self.port = port
        self.systemd_unit = systemd_unit

        self.response_times = deque(maxlen=600)
        self.memory_samples = deque(maxlen=600)

        self.latest_response_time: float | None = None
        self.last_http_status: int | None = None
        self.error_count_5min: int = 0
        self.warning_count_5min: int = 0
        self.pending_count: int = 0
        self.request_count_5min: int = 0
        self.restart_count_24h: int = 0
        self.uptime_str: str | None = None
        self.active_providers: list[str] = []
        self.child_process_count: int = 0
        self.current_pid: int | None = None
        self.last_collected: float = 0.0

    async def collect(self, http_client: httpx.AsyncClient):
        await asyncio.to_thread(self._find_pid)
        await self._measure_latency(http_client)
        await asyncio.to_thread(self._collect_memory)
        await self._parse_journal_async()
        await asyncio.to_thread(self._count_children)
        await asyncio.to_thread(self._calc_uptime)
        self.last_collected = time.time()

    def _find_pid(self):
        self.current_pid = None
        try:
            for conn in psutil.net_connections(kind="inet"):
                if conn.status == "LISTEN" and conn.laddr.port == self.port and conn.pid:
                    self.current_pid = conn.pid
                    return
        except Exception:
            pass

        if self.systemd_unit:
            try:
                out = subprocess.run(
                    ["systemctl", "show", self.systemd_unit, "--property=MainPID", "--value"],
                    capture_output=True, text=True, timeout=2
                )
                pid = int(out.stdout.strip())
                if pid > 0:
                    self.current_pid = pid
                    return
            except Exception:
                pass

        try:
            for p in psutil.process_iter(["pid", "name"]):
                name = (p.info["name"] or "").lower()
                if self.key == "9router" and "9router" in name:
                    self.current_pid = p.info["pid"]
                    return
                if self.key == "hermes" and "hermes" in name:
                    self.current_pid = p.info["pid"]
                    return
        except Exception:
            pass

    async def _measure_latency(self, http_client: httpx.AsyncClient):
        try:
            start = time.time()
            r = await http_client.get(f"http://localhost:{self.port}/", follow_redirects=True)
            elapsed = round((time.time() - start) * 1000, 1)
            self.latest_response_time = elapsed
            self.last_http_status = r.status_code
            self.response_times.append({"t": time.time(), "value": elapsed})
        except Exception:
            self.latest_response_time = None
            self.last_http_status = None

    def _collect_memory(self):
        if not self.current_pid:
            return
        try:
            p = psutil.Process(self.current_pid)
            total_rss = p.memory_info().rss
            for child in p.children(recursive=True):
                try:
                    total_rss += child.memory_info().rss
                except Exception:
                    pass
            mem_mb = round(total_rss / (1024 ** 2), 1)
            self.memory_samples.append({"t": time.time(), "value": mem_mb})
        except Exception:
            pass

    def _journal_cmd(self, extra: list[str] | None = None) -> list[str]:
        if self.systemd_unit:
            cmd = ["/usr/bin/journalctl", "-u", self.systemd_unit, "--no-pager", "-o", "short-iso"]
        elif self.current_pid:
            cmd = ["/usr/bin/journalctl", f"_PID={self.current_pid}", "--no-pager", "-o", "short-iso"]
        else:
            return []
        if extra:
            cmd.extend(extra)
        return cmd

    async def _parse_journal_async(self):
        cmd = self._journal_cmd(["--since", "5 min ago"])
        if not cmd:
            return
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=4)
            raw = stdout.decode("utf-8", errors="replace").strip()
            lines = raw.split("\n") if raw else []
            content = [l for l in lines if not l.startswith("--")]

            self.error_count_5min = sum(1 for l in content if re.search(r'\b(error|fail|critical|traceback)\b', l, re.I))
            self.warning_count_5min = sum(1 for l in content if re.search(r'\b(warn|warning)\b', l, re.I))
            self.pending_count = sum(1 for l in content if "[PENDING]" in l or re.search(r'\bpending\b', l, re.I))
            self.request_count_5min = len(content)

            if self.key == "9router":
                pset = set()
                for l in content:
                    for pattern in [r'\[CAVEMAN\].*?\|\s*(\S+)', r'\[PENDING\]\s*(?:START|END)\s*\|\s*provider=(\S+)', r'\[REQUEST\]\s*(\S+)']:
                        m = re.search(pattern, l)
                        if m:
                            pset.add(m.group(1))
                if pset:
                    self.active_providers = sorted(pset)

            cmd24 = self._journal_cmd(["--since", "24 hours ago"])
            if cmd24:
                proc24 = await asyncio.create_subprocess_exec(
                    *cmd24, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                stdout24, _ = await asyncio.wait_for(proc24.communicate(), timeout=4)
                raw24 = stdout24.decode("utf-8", errors="replace").strip()
                lines24 = raw24.split("\n") if raw24 else []
                self.restart_count_24h = sum(1 for l in lines24 if re.search(r'\b(Started|Starting|started)\b', l, re.I))
        except Exception:
            pass

    def _count_children(self):
        if not self.current_pid:
            return
        try:
            p = psutil.Process(self.current_pid)
            self.child_process_count = len(p.children(recursive=True))
        except Exception:
            pass

    def _calc_uptime(self):
        if not self.current_pid:
            return
        try:
            p = psutil.Process(self.current_pid)
            ct = datetime.fromtimestamp(p.create_time(), tz=VN_TZ)
            self.uptime_str = str(datetime.now(VN_TZ) - ct).split(".")[0]
        except Exception:
            pass

    def snapshot(self) -> dict:
        current_mem = self.memory_samples[-1]["value"] if self.memory_samples else None
        return {
            "pid": self.current_pid,
            "response_time_ms": self.latest_response_time,
            "http_status": self.last_http_status,
            "error_count_5min": self.error_count_5min,
            "warning_count_5min": self.warning_count_5min,
            "pending_count": self.pending_count,
            "request_count_5min": self.request_count_5min,
            "restart_count_24h": self.restart_count_24h,
            "uptime": self.uptime_str,
            "memory_mb": current_mem,
            "child_process_count": self.child_process_count,
            "active_providers": self.active_providers if self.key == "9router" else [],
            "last_collected": self.last_collected,
            "history": {
                "response_times": list(self.response_times),
                "memory_samples": list(self.memory_samples),
            },
        }


def get_system_info():
    """Heavy synchronous OS metrics extraction."""
    cpu_percent = psutil.cpu_percent(interval=0.1)
    cpu_freq = psutil.cpu_freq()
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    disk = psutil.disk_usage("/")
    net = psutil.net_io_counters()
    boot = datetime.fromtimestamp(psutil.boot_time(), tz=VN_TZ)
    uptime_sec = time.time() - psutil.boot_time()
    days, hours, mins = int(uptime_sec // 86400), int((uptime_sec % 86400) // 3600), int((uptime_sec % 3600) // 60)
    per_core = psutil.cpu_percent(interval=0, percpu=True)

    procs = []
    for p in psutil.process_iter(["pid", "name", "cpu_percent", "memory_percent", "memory_info", "cmdline"]):
        try:
            info = p.info
            if info["cpu_percent"] and info["cpu_percent"] > 0:
                cmd = " ".join(info["cmdline"][:3]) if info["cmdline"] else info["name"]
                info["cmd"] = cmd[:80]
                info["memory_mb"] = round(info["memory_info"].rss / (1024**2), 1) if info["memory_info"] else 0
                procs.append(info)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    procs.sort(key=lambda x: x["cpu_percent"], reverse=True)

    return {
        "ts": datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "uptime": f"{days}d {hours}h {mins}m",
        "boot_time": boot.strftime("%Y-%m-%d %H:%M:%S"),
        "cpu": {
            "percent": round(cpu_percent, 1), "cores": psutil.cpu_count(), "per_core": per_core,
            "freq_current": round(cpu_freq.current, 0) if cpu_freq else 0,
            "freq_max": round(cpu_freq.max, 0) if cpu_freq else 0,
        },
        "memory": {
            "total_gb": round(mem.total / (1024**3), 1), "used_gb": round(mem.used / (1024**3), 1),
            "available_gb": round(mem.available / (1024**3), 1), "percent": round(mem.percent, 1),
            "swap_total_gb": round(swap.total / (1024**3), 1), "swap_used_gb": round(swap.used / (1024**3), 1),
            "swap_percent": round(swap.percent, 1) if swap.total > 0 else 0,
        },
        "disk": {
            "total_gb": round(disk.total / (1024**3), 1), "used_gb": round(disk.used / (1024**3), 1),
            "free_gb": round(disk.free / (1024**3), 1), "percent": round(disk.percent, 1),
        },
        "network": {
            "bytes_sent_gb": round(net.bytes_sent / (1024**3), 2),
            "bytes_recv_gb": round(net.bytes_recv / (1024**3), 2),
        },
        "top_processes": procs[:12],
    }


async def check_service_status_async(key):
    svc = SERVICES[key]
    result = {"name": svc["name"], "description": svc["description"], "port": svc["port"],
              "status": "stopped", "pid": None, "memory_mb": 0, "cpu_percent": 0, "uptime": None,
              "systemd_status": "unknown"}

    def _check_process():
        try:
            pid = None
            for conn in psutil.net_connections(kind="inet"):
                if conn.status == "LISTEN" and conn.laddr.port == svc["port"]:
                    pid = conn.pid
                    break
            # Fallback: systemctl if net_connections gives no PID
            if not pid and svc.get("systemd"):
                try:
                    out = subprocess.run(
                        ["systemctl", "show", svc["systemd"], "--property=MainPID", "--value"],
                        capture_output=True, text=True, timeout=2
                    )
                    val = int(out.stdout.strip())
                    if val > 0:
                        pid = val
                except Exception:
                    pass
            if not pid:
                return {"status": "stopped"}
            p = psutil.Process(pid)
            ct = datetime.fromtimestamp(p.create_time(), tz=VN_TZ)
            return {
                "status": "running", "pid": pid,
                "memory_mb": round(p.memory_info().rss / (1024**2), 1),
                "cpu_percent": round(p.cpu_percent(interval=0.05), 1),
                "uptime": str(datetime.now(VN_TZ) - ct).split(".")[0]
            }
        except Exception:
            pass
        return None

    proc_data = await asyncio.to_thread(_check_process)
    if proc_data:
        result.update(proc_data)
    elif svc.get('systemd'):
        # port not found in net_connections, trust systemctl
        try:
            out = subprocess.run(
                ["systemctl", "show", svc["systemd"], "--property=MainPID", "--value"],
                capture_output=True, text=True, timeout=2
            )
            pid_str = out.stdout.strip()
            if pid_str.isdigit() and int(pid_str) > 0:
                pid = int(pid_str)
                p = psutil.Process(pid)
                ct = datetime.fromtimestamp(p.create_time(), tz=VN_TZ)
                result.update({
                    "status": "running", "pid": pid,
                    "memory_mb": round(p.memory_info().rss / (1024**2), 1),
                    "cpu_percent": round(p.cpu_percent(interval=0.05), 1),
                    "uptime": str(datetime.now(VN_TZ) - ct).split(".")[0]
                })
        except Exception:
            pass

    if svc["systemd"]:
        try:
            proc = await asyncio.create_subprocess_exec(
                "systemctl", "is-active", svc["systemd"],
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=2)
            result["systemd_status"] = stdout.decode("utf-8").strip()
        except Exception:
            pass

    try:
        async with httpx.AsyncClient(timeout=1.5) as c:
            r = await c.get(f"http://localhost:{svc['port']}/")
        result["http_status"] = r.status_code
    except Exception:
        result["http_status"] = None

    return result


def compute_health(system, services):
    warnings, criticals = [], []
    for metric, key in [("cpu", "percent"), ("memory", "percent"), ("disk", "percent")]:
        val = system[metric][key]
        if val >= THRESHOLDS[metric]["crit"]:
            criticals.append(f"{metric.upper()} at {val}%")
        elif val >= THRESHOLDS[metric]["warn"]:
            warnings.append(f"{metric.upper()} at {val}%")

    stopped_svcs = [s["name"] for s in services.values() if s["status"] == "stopped"]
    alerts = []
    if stopped_svcs:
        criticals.append(f"Services stopped: {', '.join(stopped_svcs)}")
        alerts.append(f"{len(stopped_svcs)} service(s) stopped")

    health = "critical" if criticals else ("warning" if warnings else "healthy")
    return {"status": health, "warnings": warnings, "criticals": criticals, "alerts": alerts}


async def fetch_logs_async(source: str, lines: int = 50) -> list:
    try:
        unit = LOG_SOURCES.get(source)
        cmd = ["/usr/bin/journalctl", "-n", str(lines), "--no-pager", "-o", "short-iso"]
        if unit:
            cmd.insert(2, "-u")
            cmd.insert(3, unit)

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=4)
        raw = stdout.decode("utf-8", errors="replace").strip()
        return raw.split("\n") if raw else ["(no logs)"]
    except Exception as e:
        return [f"Error fetching logs: {e}"]


monitors: dict[str, ServiceMonitor] = {}


@app.on_event("startup")
async def start_collector():
    for key, svc in SERVICES.items():
        systemd = svc.get("systemd") or (key if key != "openclaw" else None)
        monitors[key] = ServiceMonitor(key, svc["port"], systemd)

    async def _sys_run():
        while True:
            try:
                info = await asyncio.to_thread(get_system_info)
                history.append({
                    "t": time.time(), "cpu": info["cpu"]["percent"],
                    "memory": info["memory"]["percent"], "disk": info["disk"]["percent"]
                })
            except Exception:
                pass
            await asyncio.sleep(3)

    async def _svc_run():
        async with httpx.AsyncClient(timeout=4) as client:
            while True:
                await asyncio.sleep(30)
                for mon in monitors.values():
                    try:
                        await mon.collect(client)
                    except Exception:
                        pass

    asyncio.create_task(_sys_run())
    asyncio.create_task(_svc_run())


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return HTMLResponse(DASHBOARD_HTML)


@app.get("/api/all")
async def api_all():
    system = await asyncio.to_thread(get_system_info)
    tasks = {k: check_service_status_async(k) for k in SERVICES}
    services = await asyncio.gather(*tasks.values())
    services_dict = dict(zip(tasks.keys(), services))
    health = compute_health(system, services_dict)
    return JSONResponse({"system": system, "services": services_dict, "health": health, "history": list(history)})


@app.get("/api/logs/sources")
async def api_log_sources():
    return JSONResponse({"sources": list(LOG_SOURCES.keys())})


@app.get("/api/logs/recent")
async def api_logs_recent(source: str = "system", lines: int = 50):
    lines = min(max(lines, 1), 500)
    logs = await fetch_logs_async(source, lines)
    return JSONResponse({"source": source, "lines": logs})


@app.get("/api/services/{name}/metrics")
async def api_service_metrics(name: str):
    if name not in monitors:
        return JSONResponse({"error": "unknown service"}, status_code=404)
    return JSONResponse(monitors[name].snapshot())


@app.post("/api/services/{name}/restart")
async def restart_service(name: str):
    if name not in SERVICES:
        return JSONResponse({"error": "unknown"}, status_code=404)
    svc = SERVICES[name]
    sn = svc.get("systemd", name)
    try:
        proc = await asyncio.create_subprocess_exec(
            "sudo", "systemctl", "restart", sn,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await asyncio.wait_for(proc.communicate(), timeout=8)
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.websocket("/ws/logs")
async def ws_logs(ws: WebSocket):
    await ws.accept()
    proc = None
    try:
        msg = await ws.receive_text()
        data = json.loads(msg)
        source = data.get("source", "system")
        follow = data.get("follow", True)

        recent = await fetch_logs_async(source, 20)
        for line in recent:
            await ws.send_text(line)

        if not follow:
            await ws.close()
            return

        unit = LOG_SOURCES.get(source)
        cmd = ["/usr/bin/journalctl", "-f", "-n", "0", "--no-pager", "-o", "short-iso"]
        if unit:
            cmd.insert(2, "-u")
            cmd.insert(3, unit)

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )

        while True:
            line_bytes = await asyncio.wait_for(proc.stdout.readline(), timeout=30)
            if not line_bytes:
                break
            text = line_bytes.decode("utf-8", errors="replace").rstrip()
            await ws.send_text(text)

    except (asyncio.TimeoutError, WebSocketDisconnect):
        pass
    except Exception as e:
        try:
            await ws.send_text(f"Error: {e}")
        except Exception:
            pass
    finally:
        if proc:
            try:
                proc.terminate()
                await proc.wait()
            except Exception:
                pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=3333, reload=False)
