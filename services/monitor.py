import asyncio
import re
import time
import logging
import subprocess
from collections import deque
from datetime import datetime
import psutil
import httpx
from services.config import SERVICES, VN_TZ, THRESHOLDS

logger = logging.getLogger("dashboard.monitor")

def find_pid_by_port_or_systemd(port: int, systemd_unit: str | None) -> int | None:
    """Unified PID resolution strategy."""
    # 1. Systemd is the primary source of truth for managed services
    if systemd_unit:
        try:
            out = subprocess.run(
                ["systemctl", "show", systemd_unit, "--property=MainPID", "--value"],
                capture_output=True, text=True, timeout=1.5
            )
            pid_str = out.stdout.strip()
            if pid_str.isdigit():
                pid = int(pid_str)
                if pid > 0:
                    return pid
        except Exception as e:
            logger.debug(f"Failed to find PID via systemctl for {systemd_unit}: {e}")

    # 2. Fallback to scanning net connections
    try:
        for conn in psutil.net_connections(kind="inet"):
            if conn.status == "LISTEN" and conn.laddr.port == port and conn.pid:
                return conn.pid
    except (psutil.AccessDenied, Exception) as e:
        logger.debug(f"psutil.net_connections failed for port {port}: {e}")

    # 3. Fallback to process names
    try:
        unit_name = systemd_unit.split(".")[0].lower() if systemd_unit else None
        for p in psutil.process_iter(["pid", "name"]):
            name = (p.info["name"] or "").lower()
            if unit_name and unit_name in name:
                return p.info["pid"]
    except Exception as e:
        logger.debug(f"psutil process iterator failed: {e}")

    return None


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
        """Perform collection steps sequentially using threads for heavy sync OS operations."""
        try:
            self.current_pid = await asyncio.to_thread(find_pid_by_port_or_systemd, self.port, self.systemd_unit)
            await self._measure_latency(http_client)
            await asyncio.to_thread(self._collect_memory)
            await self._parse_journal_async()
            await asyncio.to_thread(self._count_children)
            await asyncio.to_thread(self._calc_uptime)
            self.last_collected = time.time()
        except Exception as e:
            logger.error(f"Failed to collect metrics for {self.key}: {e}", exc_info=True)

    async def _measure_latency(self, http_client: httpx.AsyncClient):
        try:
            start = time.time()
            r = await http_client.get(f"http://localhost:{self.port}/", follow_redirects=True, timeout=2.0)
            elapsed = round((time.time() - start) * 1000, 1)
            self.latest_response_time = elapsed
            self.last_http_status = r.status_code
            self.response_times.append({"t": time.time(), "value": elapsed})
        except Exception as e:
            logger.debug(f"Latency check failed for {self.key}: {e}")
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
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            mem_mb = round(total_rss / (1024 ** 2), 1)
            self.memory_samples.append({"t": time.time(), "value": mem_mb})
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            logger.debug(f"Memory check access issue for PID {self.current_pid}: {e}")
        except Exception as e:
            logger.warning(f"Error checking memory for {self.key}: {e}")

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
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=3.0)
            if proc.returncode != 0:
                logger.warning(f"journalctl exited with {proc.returncode}: {stderr.decode()}")
                return

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

            # Check restarts over 24h
            cmd24 = self._journal_cmd(["--since", "24 hours ago"])
            if cmd24:
                proc24 = await asyncio.create_subprocess_exec(
                    *cmd24, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                stdout24, _ = await asyncio.wait_for(proc24.communicate(), timeout=3.0)
                raw24 = stdout24.decode("utf-8", errors="replace").strip()
                lines24 = raw24.split("\n") if raw24 else []
                self.restart_count_24h = sum(1 for l in lines24 if re.search(r'\b(Started|Starting|started)\b', l, re.I))
        except asyncio.TimeoutError:
            logger.warning(f"Journalctl check timed out for {self.key}")
        except Exception as e:
            logger.warning(f"Failed to parse journalctl logs for {self.key}: {e}")

    def _count_children(self):
        if not self.current_pid:
            return
        try:
            p = psutil.Process(self.current_pid)
            self.child_process_count = len(p.children(recursive=True))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            self.child_process_count = 0
        except Exception as e:
            logger.debug(f"Error counting children for PID {self.current_pid}: {e}")

    def _calc_uptime(self):
        if not self.current_pid:
            self.uptime_str = None
            return
        try:
            p = psutil.Process(self.current_pid)
            ct = datetime.fromtimestamp(p.create_time(), tz=VN_TZ)
            self.uptime_str = str(datetime.now(VN_TZ) - ct).split(".")[0]
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            self.uptime_str = None
        except Exception as e:
            logger.debug(f"Error calculating uptime for PID {self.current_pid}: {e}")

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


async def check_service_status_async(key: str) -> dict:
    """Inspects a service immediately and returns its live state."""
    svc = SERVICES[key]
    result = {
        "name": svc["name"], 
        "description": svc["description"], 
        "port": svc["port"],
        "status": "stopped", 
        "pid": None, 
        "memory_mb": 0, 
        "cpu_percent": 0, 
        "uptime": None,
        "systemd_status": "unknown",
        "http_status": None
    }

    pid = await asyncio.to_thread(find_pid_by_port_or_systemd, svc["port"], svc.get("systemd"))
    if pid:
        try:
            p = psutil.Process(pid)
            ct = datetime.fromtimestamp(p.create_time(), tz=VN_TZ)
            result.update({
                "status": "running", 
                "pid": pid,
                "memory_mb": round(p.memory_info().rss / (1024**2), 1),
                "cpu_percent": round(p.cpu_percent(interval=0.05), 1),
                "uptime": str(datetime.now(VN_TZ) - ct).split(".")[0]
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        except Exception as e:
            logger.debug(f"Failed to check process {pid} metrics: {e}")

    # Check systemd status
    if svc.get("systemd"):
        try:
            proc = await asyncio.create_subprocess_exec(
                "systemctl", "is-active", svc["systemd"],
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=2.0)
            result["systemd_status"] = stdout.decode("utf-8").strip()
        except Exception as e:
            logger.warning(f"Failed to run systemctl is-active for {svc['systemd']}: {e}")

    # Check HTTP endpoint status
    try:
        async with httpx.AsyncClient(timeout=1.5) as c:
            r = await c.get(f"http://localhost:{svc['port']}/")
            result["http_status"] = r.status_code
    except Exception as e:
        logger.debug(f"HTTP connection failed to http://localhost:{svc['port']}/ : {e}")

    return result


def compute_health(system: dict, services: dict) -> dict:
    """Computes overall system state based on resource usage and service states."""
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
