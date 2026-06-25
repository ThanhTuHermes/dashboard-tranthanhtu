import asyncio
import os
import time
import logging
from collections import deque
from pathlib import Path
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Depends, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# Import Services
from services.config import SERVICES, MAX_HISTORY, SYSTEM_INTERVAL, MONITOR_INTERVAL, DASHBOARD_USERNAME, DASHBOARD_PASSWORD, DASHBOARD_API_KEY
from services.auth import SESSION_TOKEN, verify_credentials, verify_token, verify_websocket
from services.system import get_system_info
from services.monitor import ServiceMonitor, check_service_status_async, compute_health
from services.logging import fetch_logs_async, stream_logs_websocket

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("dashboard.main")

# Load template
DASHBOARD_HTML_PATH = Path(__file__).parent / "templates" / "dashboard.html"
DASHBOARD_HTML = DASHBOARD_HTML_PATH.read_text(encoding="utf-8") if DASHBOARD_HTML_PATH.exists() else "<h1>Dashboard Template Not Found</h1>"

# Cache store
cached_dashboard_data = {
    "system": None,
    "services": {},
    "health": {"status": "unknown", "warnings": [], "criticals": [], "alerts": []},
    "history": []
}

# Deque for history tracking
history = deque(maxlen=MAX_HISTORY)
monitors: dict[str, ServiceMonitor] = {}

async def _sys_run_loop():
    """Background loop collecting system metrics."""
    while True:
        try:
            # force_refresh=True ensures we fetch fresh OS metrics instead of cache
            info = await asyncio.to_thread(get_system_info, force_refresh=True)
            
            history.append({
                "t": time.time(), 
                "cpu": info["cpu"]["percent"],
                "memory": info["memory"]["percent"], 
                "disk": info["disk"]["percent"]
            })
            
            cached_dashboard_data["system"] = info
            cached_dashboard_data["history"] = list(history)
            
            # Recompute health if services metadata is populated
            if cached_dashboard_data["services"]:
                cached_dashboard_data["health"] = compute_health(
                    cached_dashboard_data["system"], 
                    cached_dashboard_data["services"]
                )
        except Exception as e:
            logger.error(f"Error in background system info collector: {e}")
        await asyncio.sleep(SYSTEM_INTERVAL)

async def _svc_run_loop():
    """Background loop collecting service status and deep metrics."""
    async with httpx.AsyncClient(timeout=4.0) as client:
        while True:
            try:
                # 1. Update service monitors (response time & memory samples)
                for mon in monitors.values():
                    try:
                        await mon.collect(client)
                    except Exception as e:
                        logger.debug(f"Failed to collect detailed metrics for service {mon.key}: {e}")
                
                # 2. Update service live status (PIDs, CPU, etc.)
                tasks = {k: check_service_status_async(k) for k in SERVICES}
                services_states = await asyncio.gather(*tasks.values())
                services_dict = dict(zip(tasks.keys(), services_states))
                
                cached_dashboard_data["services"] = services_dict
                
                if cached_dashboard_data["system"]:
                    cached_dashboard_data["health"] = compute_health(
                        cached_dashboard_data["system"], 
                        cached_dashboard_data["services"]
                    )
            except Exception as e:
                logger.error(f"Error in background services status collector: {e}")
            await asyncio.sleep(MONITOR_INTERVAL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    logger.info("=" * 60)
    logger.info(f"Dashboard Auth Username: {DASHBOARD_USERNAME}")
    logger.info(f"Dashboard Auth Password: {DASHBOARD_PASSWORD}")
    logger.info(f"Dashboard Auth API Key:  {DASHBOARD_API_KEY}")
    logger.info(f"Generated Session Token: {SESSION_TOKEN}")
    logger.info("=" * 60)
    logger.info("Starting Dashboard Monitor services...")
    for key, svc in SERVICES.items():
        systemd = svc.get("systemd") or (key if key != "openclaw" else None)
        monitors[key] = ServiceMonitor(key, svc["port"], systemd)
    
    # Pre-populate cache once so the app is instantly usable
    try:
        initial_sys = await asyncio.to_thread(get_system_info, force_refresh=True)
        cached_dashboard_data["system"] = initial_sys
        
        tasks = {k: check_service_status_async(k) for k in SERVICES}
        services_states = await asyncio.gather(*tasks.values())
        cached_dashboard_data["services"] = dict(zip(tasks.keys(), services_states))
        
        cached_dashboard_data["health"] = compute_health(
            cached_dashboard_data["system"], 
            cached_dashboard_data["services"]
        )
        logger.info("Initial dashboard cache populated successfully.")
    except Exception as e:
        logger.error(f"Failed to populate initial cache: {e}")
        
    # Start background loops
    sys_task = asyncio.create_task(_sys_run_loop())
    svc_task = asyncio.create_task(_svc_run_loop())
    
    yield
    
    # --- Shutdown ---
    logger.info("Stopping Dashboard Monitor background tasks...")
    sys_task.cancel()
    svc_task.cancel()
    await asyncio.gather(sys_task, svc_task, return_exceptions=True)
    logger.info("Dashboard Monitor stopped.")


# Initialize FastAPI app with lifespan
app = FastAPI(title="Dashboard Monitor - TranThanhTu.site", lifespan=lifespan)

# Mount static files if directory exists
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


# --- HTTP ROUTES ---

@app.get("/", response_class=HTMLResponse)
async def dashboard_ui(request: Request, username: str = Depends(verify_credentials)):
    """Serves the dashboard HTML page with the injected current session token."""
    html_content = DASHBOARD_HTML.replace("{{SESSION_TOKEN}}", SESSION_TOKEN)
    return HTMLResponse(html_content)


@app.get("/api/all", dependencies=[Depends(verify_token)])
async def api_all():
    """Returns cached system metrics and active services status."""
    return JSONResponse(cached_dashboard_data)


@app.get("/api/logs/recent", dependencies=[Depends(verify_token)])
async def api_logs_recent(source: str = "system", lines: int = 50):
    """Fetches recent lines from systemd journalctl or system logs."""
    lines = min(max(lines, 1), 500)
    logs = await fetch_logs_async(source, lines)
    return JSONResponse({"source": source, "lines": logs})


@app.get("/api/services/{name}/metrics", dependencies=[Depends(verify_token)])
async def api_service_metrics(name: str):
    """Returns a snapshot of the detailed metrics history for a given service."""
    if name not in monitors:
        return JSONResponse({"error": "unknown service"}, status_code=status.HTTP_404_NOT_FOUND)
    return JSONResponse(monitors[name].snapshot())


@app.post("/api/services/{name}/restart", dependencies=[Depends(verify_token)])
async def restart_service(name: str):
    """Restarts a systemd service securely."""
    if name not in SERVICES:
        return JSONResponse({"error": "unknown service"}, status_code=status.HTTP_404_NOT_FOUND)
    
    svc = SERVICES[name]
    sn = svc.get("systemd", name)
    logger.info(f"Initiating systemd service restart for: {name} (systemd: {sn})")
    
    try:
        proc = await asyncio.create_subprocess_exec(
            "sudo", "systemctl", "restart", sn,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=8.0)
        
        if proc.returncode != 0:
            err_msg = stderr.decode("utf-8", errors="replace").strip()
            logger.error(f"Restart failed for service {name}: {err_msg}")
            return JSONResponse({"status": "error", "message": err_msg}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        logger.info(f"Service {name} restarted successfully.")
        return JSONResponse({"status": "ok"})
    except asyncio.TimeoutError:
        logger.error(f"Restart timed out for service {name}")
        return JSONResponse({"status": "error", "message": "Service restart timed out"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    except Exception as e:
        logger.error(f"Error occurred while restarting service {name}: {e}", exc_info=True)
        return JSONResponse({"status": "error", "message": str(e)}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


# --- WEBSOCKETS ---

@app.websocket("/ws/logs")
async def ws_logs(ws: WebSocket):
    """Secured WebSocket endpoint to stream logs in real-time."""
    await ws.accept()
    
    # 1. Verify credentials via WebSocket handshake query param or first auth message
    if not await verify_websocket(ws):
        logger.warning("Rejected unauthenticated WebSocket log request.")
        try:
            await ws.send_text("Error: Unauthorized")
            await ws.close(code=status.WS_1008_POLICY_VIOLATION)
        except Exception:
            pass
        return
        
    try:
        # Wait for the client setup message (contains source and follow options)
        msg = await ws.receive_text()
        data = json.loads(msg)
        source = data.get("source", "system")
        follow = data.get("follow", True)

        await stream_logs_websocket(ws, source, follow)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.warning(f"Error in WebSocket logs connection: {e}")


if __name__ == "__main__":
    import uvicorn
    # Log the security credentials to terminal at startup for ease of discovery
    logger.info("=" * 60)
    logger.info(f"Dashboard Auth Username: {DASHBOARD_USERNAME}")
    logger.info(f"Dashboard Auth Password: {DASHBOARD_PASSWORD}")
    logger.info(f"Dashboard Auth API Key:  {DASHBOARD_API_KEY}")
    logger.info(f"Generated Session Token: {SESSION_TOKEN}")
    logger.info("=" * 60)
    
    uvicorn.run("app:app", host="0.0.0.0", port=3333, reload=False)
