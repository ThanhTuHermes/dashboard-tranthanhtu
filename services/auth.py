import secrets
from fastapi import Depends, HTTPException, status, WebSocket
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.security.api_key import APIKeyHeader, APIKeyQuery
from services.config import DASHBOARD_USERNAME, DASHBOARD_PASSWORD, DASHBOARD_API_KEY

security_basic = HTTPBasic()
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
api_key_query = APIKeyQuery(name="token", auto_error=False)

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
    token_header: str = Depends(api_key_header),
    token_query: str = Depends(api_key_query),
    credentials: HTTPBasicCredentials = Depends(HTTPBasic(auto_error=False))
):
    # Try Bearer/Header token first
    if token_header:
        if token_header.startswith("Bearer "):
            token_val = token_header[7:]
        else:
            token_val = token_header
        if secrets.compare_digest(token_val, SESSION_TOKEN) or secrets.compare_digest(token_val, DASHBOARD_API_KEY):
            return True

    # Try Query Parameter token next
    if token_query:
        if secrets.compare_digest(token_query, SESSION_TOKEN) or secrets.compare_digest(token_query, DASHBOARD_API_KEY):
            return True

    # Try Basic Auth fallback
    if credentials:
        correct_username = secrets.compare_digest(credentials.username, DASHBOARD_USERNAME)
        correct_password = secrets.compare_digest(credentials.password, DASHBOARD_PASSWORD)
        if correct_username and correct_password:
            return True

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid credentials or session token",
    )

async def verify_websocket(websocket: WebSocket) -> bool:
    """Validate WS connection credentials using query params or headers."""
    token = websocket.query_params.get("token")
    if token and (secrets.compare_digest(token, SESSION_TOKEN) or secrets.compare_digest(token, DASHBOARD_API_KEY)):
        return True
        
    # Check headers (Authorization or Sec-WebSocket-Protocol / X-API-Key)
    auth_header = websocket.headers.get("authorization")
    if auth_header:
        if auth_header.startswith("Bearer "):
            token_val = auth_header[7:]
        elif auth_header.startswith("Basic "):
            # Decrypt basic auth if present
            import base64
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
