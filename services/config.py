import os
from datetime import timezone, timedelta
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Authentication config
DASHBOARD_USERNAME = os.getenv("DASHBOARD_USERNAME", "admin")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD")
DASHBOARD_API_KEY = os.getenv("DASHBOARD_API_KEY")

# Fail-fast check to prevent running with missing credentials in production
if not DASHBOARD_PASSWORD or not DASHBOARD_API_KEY:
    raise ValueError(
        "CRITICAL SECURITY CONFIGURATION ERROR: Both 'DASHBOARD_PASSWORD' and 'DASHBOARD_API_KEY' "
        "must be defined in the environment or in the .env file. "
        "Using default credentials or running without them is disallowed."
    )

# Timezone config
VN_TZ = timezone(timedelta(hours=7))

# Monitoring intervals (seconds)
MONITOR_INTERVAL = int(os.getenv("MONITOR_INTERVAL", "30"))
SYSTEM_INTERVAL = int(os.getenv("SYSTEM_INTERVAL", "3"))
MAX_HISTORY = 60

# Monitored services config (explicit systemd configurations to avoid convention bugs)
SERVICES = {
    "openclaw": {
        "name": "OpenClaw Gateway", 
        "port": 18789, 
        "systemd": None,
        "description": "AI Agent Gateway", 
        "monitoring_depth": "full",
    },
    "hermes": {
        "name": "Hermes Agent", 
        "port": 9119, 
        "systemd": "hermes.service",
        "description": "Hermes AI Dashboard & LSP", 
        "monitoring_depth": "full",
    },
    "9router": {
        "name": "9Router AI Gateway", 
        "port": 20128, 
        "systemd": "9router.service",
        "description": "AI Model Router", 
        "monitoring_depth": "full",
    },
}

# Systemd services to fetch logs from
LOG_SOURCES = {
    "system": None,
    "openclaw": None,
    "hermes": "hermes.service",
    "9router": "9router.service",
    "dashboard": "dashboard.service",
    "cloudflared": "cloudflared.service",
}

# Alerts and health thresholds
THRESHOLDS = {
    "cpu": {"warn": 60, "crit": 80},
    "memory": {"warn": 70, "crit": 85},
    "disk": {"warn": 75, "crit": 90},
}
