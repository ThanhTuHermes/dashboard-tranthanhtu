import secrets
import base64
from fastapi import Depends, HTTPException, status, WebSocket, Cookie, Header, Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from services.config import DASHBOARD_USERNAME, DASHBOARD_PASSWORD, DASHBOARD_API_KEY

security_basic = HTTPBasic()

# Dynamically generated session token for the current UI session
SESSION_TOKEN = secrets.token_urlsafe(32)

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security_basic)):
    correct_username = secrets.compare_digest(credentials.username, DASHBOARD_USERNAME)
    correct_password = secrets.compare_digest(credentials.password, DASHBOARD_PASSWORD)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

def verify_token(
    session_id: str | None = Cookie(default=None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    token_query: str | None = Query(default=None, alias="token")
):
    # 1. Validate HTTP-only Cookie session_id first (secure method for browser fetch requests)
    if session_id and secrets.compare_digest(session_id, SESSION_TOKEN):
        return True

    # 2. Validate X-API-Key header (for automated scripts/test clients)
    if x_api_key and secrets.compare_digest(x_api_key, DASHBOARD_API_KEY):
        return True

    # 3. Validate query parameter fallback (for programmatic clients/metrics scraping)
    if token_query and secrets.compare_digest(token_query, DASHBOARD_API_KEY):
        return True

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Unauthorized access. Invalid credentials or expired session.",
    )

async def verify_websocket(websocket: WebSocket) -> bool:
    """Validate WS connection credentials before handshake acceptance."""
    # 1. Check HTTP-only cookie (primary, most secure browser-native method)
    session_cookie = websocket.cookies.get("session_id")
    if session_cookie and secrets.compare_digest(session_cookie, SESSION_TOKEN):
        return True

    # 2. Fallback check: query param (useful for development or programmatic WebSocket clients)
    token_query = websocket.query_params.get("token")
    if token_query and (secrets.compare_digest(token_query, SESSION_TOKEN) or secrets.compare_digest(token_query, DASHBOARD_API_KEY)):
        return True

    # 3. Check authorization header if present in handshake headers
    auth_header = websocket.headers.get("authorization")
    if auth_header:
        if auth_header.startswith("Bearer "):
            token_val = auth_header[7:]
        elif auth_header.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
                username, password = decoded.split(":", 1)
                if secrets.compare_digest(username, DASHBOARD_USERNAME) and secrets.compare_digest(password, DASHBOARD_PASSWORD):
                    return True
            except Exception:
                pass
            token_val = None
        else:
            token_val = auth_header

        if token_val and (secrets.compare_digest(token_val, SESSION_TOKEN) or secrets.compare_digest(token_val, DASHBOARD_API_KEY)):
            return True

    return False
