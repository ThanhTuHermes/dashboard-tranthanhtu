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
app.mount("/static", StaticFiles(directory="static"), name="static")

DASHBOARD_HTML = (Path(__file__).parent / "templates" / "dashboard.html").read_text()

VN_TZ = timezone(timedelta(hours=7))

# ─── History buffer (for charts) ──────────────────────────────────
MAX_HISTORY = 60
history = deque(maxlen=MAX_HISTORY)

# ─── Log tailing ─────────────────────────────────────────────────
# Track active log readers so we can reconnect cleanly
# structure: {client_id: proc_ref}

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

# Log sources for WebSocket
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

# ─── ServiceMonitor - Deep per-service monitoring ────────────────

class ServiceMonitor:
    """Per-service deep monitoring: latency, memory, logs, uptime."""

    def __init__(self, key: str, port: int, systemd_unit: str | None):
        self.key = key
        self.port = port
        self.systemd_unit = systemd_unit

        # Trend data (deques maxlen=600 = 5h at 30s intervals)
        self.response_times = deque(maxlen=600)
        self.memory_samples = deque(maxlen=600)

        # Current snapshot
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
        """Run one collection cycle."""
        self._find_pid()
        await self._measure_latency(http_client)
        self._collect_memory()
        await asyncio.to_thread(self._parse_journal)
        self._count_children()
        self._calc_uptime()
        self.last_collected = time.time()

    def _find_pid(self):
        self.current_pid = None
        # Try net_connections first
        for conn in psutil.net_connections(kind="inet"):
            if conn.status == "LISTEN" and conn.laddr.port == self.port and conn.pid:
                self.current_pid = conn.pid
                return
        # Fallback: systemctl for systemd services
        if self.systemd_unit:
            try:
                out = subprocess.run(
                    ["systemctl", "show", self.systemd_unit, "--property=MainPID", "--value"],
                    capture_output=True, text=True, timeout=3
                )
                pid = int(out.stdout.strip())
                if pid > 0:
                    self.current_pid = pid
            except Exception:
                pass
        # Fallback: psutil process scan
        if not self.current_pid:
            for p in psutil.process_iter(["pid", "name"]):
                try:
                    if self.key == "9router" and "9router" in (p.info["name"] or ""):
                        self.current_pid = p.info["pid"]
                        return
                    if self.key == "hermes" and "hermes" in (p.info["name"].lower() or ""):
                        self.current_pid = p.info["pid"]
                        return
                except (psutil.NoSuchProcess, psutil.AccessDenied):
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
            for child in p.children():
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
            cmd = ["journalctl", "-u", self.systemd_unit, "--no-pager", "-o", "short-iso"]
        elif self.current_pid:
            cmd = ["journalctl", f"_PID={self.current_pid}", "--no-pager", "-o", "short-iso"]
        else:
            return []
        if extra:
            cmd.extend(extra)
        return cmd

    def _parse_journal(self):
        try:
            cmd = self._journal_cmd(["--since", "5 min ago"])
            if not cmd:
                return
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            raw = out.stdout.strip()
            lines = raw.split("\n") if raw else []
            content = [l for l in lines if not l.startswith("--")]

            self.error_count_5min = sum(1 for l in content
                                        if re.search(r'\b(error|fail|critical|traceback)\b', l, re.I))
            self.warning_count_5min = sum(1 for l in content
                                          if re.search(r'\b(warn|warning)\b', l, re.I))
            self.pending_count = sum(1 for l in content
                                     if "[PENDING]" in l or re.search(r'\bpending\b', l, re.I))
            self.request_count_5min = len(content)

            # 9Router: parse active providers
            if self.key == "9router":
                pset = set()
                for l in content:
                    m = re.search(r'\[CAVEMAN\].*?\|\s*(\S+)', l)
                    if m:
                        pset.add(m.group(1))
                    m = re.search(r'\[PENDING\]\s*(?:START|END)\s*\|\s*provider=(\S+)', l)
                    if m:
                        pset.add(m.group(1))
                    m = re.search(r'\[REQUEST\]\s*(\S+)', l)
                    if m:
                        pset.add(m.group(1))
                if pset:
                    self.active_providers = sorted(pset)

            # Restart count in 24h
            cmd24 = self._journal_cmd(["--since", "24 hours ago"])
            if cmd24:
                out24 = subprocess.run(cmd24, capture_output=True, text=True, timeout=5)
                raw24 = out24.stdout.strip()
                lines24 = raw24.split("\n") if raw24 else []
                self.restart_count_24h = sum(1 for l in lines24
                                             if re.search(r'\b(Started|Starting|started)\b', l, re.I))
        except Exception:
            pass

    def _count_children(self):
        if not self.current_pid:
            return
        try:
            p = psutil.Process(self.current_pid)
            self.child_process_count = len(p.children())
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
        """Return current metrics snapshot (lightweight, no I/O)."""
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

# ─── System info ─────────────────────────────────────────────────

def get_system_info():
    cpu_percent = psutil.cpu_percent(interval=0.3)
    cpu_freq = psutil.cpu_freq()
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    disk = psutil.disk_usage("/")
    net = psutil.net_io_counters()
    boot = datetime.fromtimestamp(psutil.boot_time(), tz=VN_TZ)
    uptime_sec = time.time() - psutil.boot_time()
    days = int(uptime_sec // 86400)
    hours = int((uptime_sec % 86400) // 3600)
    mins = int((uptime_sec % 3600) // 60)
    per_core = psutil.cpu_percent(interval=0, percpu=True)

    # Top processes by CPU
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
    top_procs = procs[:12]

    return {
        "ts": datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "uptime": f"{days}d {hours}h {mins}m",
        "boot_time": boot.strftime("%Y-%m-%d %H:%M:%S"),
        "cpu": {
            "percent": round(cpu_percent, 1), "cores": psutil.cpu_count(),
            "per_core": per_core,
            "freq_current": round(cpu_freq.current, 0) if cpu_freq else 0,
            "freq_max": round(cpu_freq.max, 0) if cpu_freq else 0,
        },
        "memory": {
            "total_gb": round(mem.total / (1024**3), 1),
            "used_gb": round(mem.used / (1024**3), 1),
            "available_gb": round(mem.available / (1024**3), 1),
            "percent": round(mem.percent, 1),
            "swap_total_gb": round(swap.total / (1024**3), 1),
            "swap_used_gb": round(swap.used / (1024**3), 1),
            "swap_percent": round(swap.percent, 1) if swap.total > 0 else 0,
        },
        "disk": {
            "total_gb": round(disk.total / (1024**3), 1),
            "used_gb": round(disk.used / (1024**3), 1),
            "free_gb": round(disk.free / (1024**3), 1),
            "percent": round(disk.percent, 1),
        },
        "network": {
            "bytes_sent_gb": round(net.bytes_sent / (1024**3), 2),
            "bytes_recv_gb": round(net.bytes_recv / (1024**3), 2),
        },
        "top_processes": top_procs,
    }


def check_service_status(key):
    svc = SERVICES[key]
    result = {"name": svc["name"], "description": svc["description"], "port": svc["port"],
              "status": "stopped", "pid": None, "memory_mb": 0, "cpu_percent": 0, "uptime": None,
              "systemd_status": None}
    for conn in psutil.net_connections(kind="inet"):
        if conn.status == "LISTEN" and conn.laddr.port == svc["port"]:
            result["status"] = "running"
            if conn.pid:
                try:
                    p = psutil.Process(conn.pid)
                    result["pid"] = conn.pid
                    result["memory_mb"] = round(p.memory_info().rss / (1024**2), 1)
                    result["cpu_percent"] = round(p.cpu_percent(interval=0.1), 1)
                    ct = datetime.fromtimestamp(p.create_time(), tz=VN_TZ)
                    result["uptime"] = str(datetime.now(VN_TZ) - ct).split(".")[0]
                except Exception:
                    pass
            break
    if svc["systemd"]:
        try:
            out = subprocess.run(["systemctl", "is-active", svc["systemd"]],
                                 capture_output=True, text=True, timeout=3)
            result["systemd_status"] = out.stdout.strip()
        except Exception:
            result["systemd_status"] = "unknown"
    try:
        with httpx.Client(timeout=2) as c:
            r = c.get(f"http://localhost:{svc['port']}/")
            result["http_status"] = r.status_code
    except Exception:
        result["http_status"] = None
    return result


def compute_health(system, services):
    warnings, criticals = [], []
    if system["cpu"]["percent"] >= THRESHOLDS["cpu"]["crit"]:
        criticals.append(f"CPU at {system['cpu']['percent']}%")
    elif system["cpu"]["percent"] >= THRESHOLDS["cpu"]["warn"]:
        warnings.append(f"CPU at {system['cpu']['percent']}%")
    if system["memory"]["percent"] >= THRESHOLDS["memory"]["crit"]:
        criticals.append(f"Memory at {system['memory']['percent']}%")
    elif system["memory"]["percent"] >= THRESHOLDS["memory"]["warn"]:
        warnings.append(f"Memory at {system['memory']['percent']}%")
    if system["disk"]["percent"] >= THRESHOLDS["disk"]["crit"]:
        criticals.append(f"Disk at {system['disk']['percent']}%")
    elif system["disk"]["percent"] >= THRESHOLDS["disk"]["warn"]:
        warnings.append(f"Disk at {system['disk']['percent']}%")
    stopped_svcs = [s["name"] for s in services.values() if s["status"] == "stopped"]
    alerts = []
    if stopped_svcs:
        criticals.append(f"Services stopped: {', '.join(stopped_svcs)}")
        alerts.append(f"{len(stopped_svcs)} service(s) stopped")
    if criticals:
        health = "critical"
    elif warnings:
        health = "warning"
    else:
        health = "healthy"
    return {"status": health, "warnings": warnings, "criticals": criticals, "alerts": alerts}


# ─── Log helpers ─────────────────────────────────────────────────

def fetch_logs(source: str, lines: int = 50) -> list:
    """Fetch recent logs via journalctl."""
    try:
        unit = LOG_SOURCES.get(source)
        if source == "system" or unit is None:
            cmd = ["/usr/bin/journalctl", "-n", str(lines), "--no-pager", "-o", "short-iso"]
        else:
            cmd = ["/usr/bin/journalctl", "-u", unit, "-n", str(lines), "--no-pager", "-o", "short-iso"]
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        return out.stdout.strip().split("\n") if out.stdout.strip() else ["(no logs)"]
    except Exception as e:
        return [f"Error: {e}"]


# ─── Service monitors (deep monitoring) ─────────────────────────
monitors: dict[str, ServiceMonitor] = {}


@app.on_event("startup")
async def start_collector():
    # System metrics collector
    async def _sys_run():
        while True:
            try:
                info = get_system_info()
                history.append({"t": time.time(), "cpu": info["cpu"]["percent"],
                                "memory": info["memory"]["percent"], "disk": info["disk"]["percent"]})
            except Exception:
                pass
            await asyncio.sleep(3)
    asyncio.create_task(_sys_run())

    # Service deep-metrics collector (30s intervals)
    async def _svc_run():
        async with httpx.AsyncClient(timeout=5) as client:
            while True:
                for key, mon in monitors.items():
                    try:
                        await mon.collect(client)
                    except Exception:
                        pass
                await asyncio.sleep(30)
    asyncio.create_task(_svc_run())

    # Init monitors
    for key, svc in SERVICES.items():
        systemd = svc.get("systemd") or (key if key != "openclaw" else None)
        monitors[key] = ServiceMonitor(key, svc["port"], systemd)


# ─── REST Routes ─────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return HTMLResponse(DASHBOARD_HTML)


@app.get("/api/all")
async def api_all():
    system = get_system_info()
    services = {k: check_service_status(k) for k in SERVICES}
    health = compute_health(system, services)
    return JSONResponse({"system": system, "services": services, "health": health, "history": list(history)})


@app.get("/api/logs/sources")
async def api_log_sources():
    return JSONResponse({"sources": list(LOG_SOURCES.keys())})


@app.get("/api/logs/recent")
async def api_logs_recent(source: str = "system", lines: int = 50):
    lines = min(max(lines, 1), 500)
    return JSONResponse({"source": source, "lines": fetch_logs(source, lines)})


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
        subprocess.run(["sudo", "systemctl", "restart", sn], capture_output=True, text=True, timeout=10)
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


# ─── WebSocket ───────────────────────────────────────────────────

@app.websocket("/ws/logs")
async def ws_logs(ws: WebSocket):
    await ws.accept()
    proc = None
    try:
        msg = await ws.receive_text()
        data = json.loads(msg)
        source = data.get("source", "system")
        follow = data.get("follow", True)

        # Send recent logs first
        recent = fetch_logs(source, 20)
        for line in recent:
            await ws.send_text(line)

        if not follow:
            await ws.close()
            return

        # Stream new lines
        unit = LOG_SOURCES.get(source)
        if source == "system" or unit is None:
            cmd = ["/usr/bin/journalctl", "-f", "-n", "0", "--no-pager", "-o", "short-iso"]
        else:
            cmd = ["/usr/bin/journalctl", "-u", unit, "-f", "-n", "0", "--no-pager", "-o", "short-iso"]

        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        while True:
            line = await asyncio.wait_for(proc.stdout.readline(), timeout=30)
            if not line:
                break
            text = line.decode("utf-8", errors="replace").rstrip()
            await ws.send_text(text)

    except asyncio.TimeoutError:
        try:
            await ws.send_text("--- Connection idle, closing ---")
        except Exception:
            pass
    except WebSocketDisconnect:
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
            except Exception:
                pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=3333, reload=False)