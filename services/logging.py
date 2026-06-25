import asyncio
import logging
from fastapi import WebSocket
from services.config import LOG_SOURCES

logger = logging.getLogger("dashboard.logging")

async def fetch_logs_async(source: str, lines: int = 50) -> list:
    """Fetch logs asynchronously using journalctl with validation."""
    if source not in LOG_SOURCES:
        logger.warning(f"Rejected invalid log source lookup request: {source}")
        return [f"Error: Invalid log source '{source}'"]

    try:
        unit = LOG_SOURCES.get(source)
        cmd = ["/usr/bin/journalctl", "-n", str(lines), "--no-pager", "-o", "short-iso"]
        if unit:
            cmd.insert(2, "-u")
            cmd.insert(3, unit)

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=4.0)
        
        if proc.returncode != 0:
            err_msg = stderr.decode("utf-8", errors="replace").strip()
            logger.warning(f"journalctl returned exit code {proc.returncode} for source {source}: {err_msg}")
            return [f"Error fetching logs: journalctl exited with {proc.returncode}"]
            
        raw = stdout.decode("utf-8", errors="replace").strip()
        return raw.split("\n") if raw else ["(no logs)"]
    except asyncio.TimeoutError:
        logger.warning(f"Timeout fetching logs for source {source}")
        return ["Error: Fetching logs timed out."]
    except Exception as e:
        logger.error(f"Error fetching logs for source {source}: {e}", exc_info=True)
        return [f"Error fetching logs: {e}"]

async def stream_logs_websocket(websocket: WebSocket, source: str, follow: bool = True):
    """Streams systemd/journalctl logs directly to the WebSocket client with ping/keepalive check."""
    if source not in LOG_SOURCES:
        logger.warning(f"Rejected invalid log source WS request: {source}")
        await websocket.send_text(f"Error: Invalid log source '{source}'")
        await websocket.close()
        return

    proc = None
    try:
        # 1. Send recent 20 log lines first
        recent = await fetch_logs_async(source, 20)
        for line in recent:
            await websocket.send_text(line)

        if not follow:
            await websocket.close()
            return

        # 2. Start follow mode
        unit = LOG_SOURCES.get(source)
        cmd = ["/usr/bin/journalctl", "-f", "-n", "0", "--no-pager", "-o", "short-iso"]
        if unit:
            cmd.insert(2, "-u")
            cmd.insert(3, unit)

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )

        while True:
            try:
                # Read line with timeout
                line_bytes = await asyncio.wait_for(proc.stdout.readline(), timeout=30.0)
                if not line_bytes:
                    break
                text = line_bytes.decode("utf-8", errors="replace").rstrip()
                await websocket.send_text(text)
            except asyncio.TimeoutError:
                # Connection is idle; send a ping frame to verify client is still connected
                try:
                    await websocket.send_json({"ping": True})
                except Exception:
                    # Connection is dead; clean up and break loop
                    logger.debug("WebSocket client disconnected (heartbeat ping failed).")
                    break

    except Exception as e:
        logger.debug(f"Log streaming loop terminated: {e}")
    finally:
        if proc:
            try:
                proc.terminate()
                await proc.wait()
            except Exception:
                pass
