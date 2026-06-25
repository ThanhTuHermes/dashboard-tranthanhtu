import time
import logging
from datetime import datetime
import psutil
from services.config import VN_TZ

logger = logging.getLogger("dashboard.system")

# Simple in-memory cache for system metrics
_cached_system_info = None
_last_system_fetch_time = 0.0
CACHE_TTL = 1.0  # seconds

def get_system_info(force_refresh: bool = False) -> dict:
    """Heavy synchronous OS metrics extraction with caching."""
    global _cached_system_info, _last_system_fetch_time
    
    current_time = time.time()
    if not force_refresh and _cached_system_info and (current_time - _last_system_fetch_time < CACHE_TTL):
        return _cached_system_info

    try:
        cpu_percent = psutil.cpu_percent(interval=0.1)
        cpu_freq = psutil.cpu_freq()
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        disk = psutil.disk_usage("/")
        net = psutil.net_io_counters()
        boot = datetime.fromtimestamp(psutil.boot_time(), tz=VN_TZ)
        uptime_sec = current_time - psutil.boot_time()
        days, hours, mins = int(uptime_sec // 86400), int((uptime_sec % 86400) // 3600), int((uptime_sec % 3600) // 60)
        per_core = psutil.cpu_percent(interval=0, percpu=True)

        procs = []
        # Querying process list can raise AccessDenied for some processes
        for p in psutil.process_iter(["pid", "name", "cpu_percent", "memory_percent", "memory_info", "cmdline"]):
            try:
                info = p.info
                # Ensure cpu_percent is not None and is > 0
                if info.get("cpu_percent") is not None and info["cpu_percent"] > 0:
                    cmdline = info.get("cmdline")
                    cmd = " ".join(cmdline[:3]) if cmdline else info.get("name") or "unknown"
                    info["cmd"] = cmd[:80]
                    
                    mem_info = info.get("memory_info")
                    info["memory_mb"] = round(mem_info.rss / (1024**2), 1) if mem_info else 0.0
                    procs.append(info)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            except Exception as e:
                logger.debug(f"Error processing process info for PID: {e}")

        procs.sort(key=lambda x: x.get("cpu_percent", 0.0), reverse=True)

        info_dict = {
            "ts": datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "uptime": f"{days}d {hours}h {mins}m",
            "boot_time": boot.strftime("%Y-%m-%d %H:%M:%S"),
            "cpu": {
                "percent": round(cpu_percent, 1), 
                "cores": psutil.cpu_count(), 
                "per_core": per_core,
                "freq_current": round(cpu_freq.current, 0) if cpu_freq else 0.0,
                "freq_max": round(cpu_freq.max, 0) if cpu_freq else 0.0,
            },
            "memory": {
                "total_gb": round(mem.total / (1024**3), 1), 
                "used_gb": round(mem.used / (1024**3), 1),
                "available_gb": round(mem.available / (1024**3), 1), 
                "percent": round(mem.percent, 1),
                "swap_total_gb": round(swap.total / (1024**3), 1), 
                "swap_used_gb": round(swap.used / (1024**3), 1),
                "swap_percent": round(swap.percent, 1) if swap.total > 0 else 0.0,
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
            "top_processes": procs[:12],
        }
        
        _cached_system_info = info_dict
        _last_system_fetch_time = current_time
        return info_dict

    except Exception as e:
        logger.error(f"Error gathering system info: {e}", exc_info=True)
        # Fallback to cached or empty structure
        if _cached_system_info:
            return _cached_system_info
        raise e
