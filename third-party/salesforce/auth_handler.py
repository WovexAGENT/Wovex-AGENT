import os
import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple, List
from functools import wraps
import jwt
import bcrypt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from fastapi import HTTPException, status, Depends
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel, ValidationError
from starlette.requests import Request

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AuthHandler")

# Security constants
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 7
ALGORITHM = "RS256"
TOKEN_TYPE = "Bearer"

# Load cryptographic keys
def load_rsa_keys() -> Tuple[str, str]:
    private_key_path = os.getenv("PRIVATE_KEY_PATH", "private_key.pem")
    public_key_path = os.getenv("PUBLIC_KEY_PATH", "public_key.pem")
    
    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=os.getenv("KEY_PASSWORD").encode() if os.getenv("KEY_PASSWORD") else None,
            backend=default_backend()
        )
    
    with open(public_key_path, "rb") as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read(),
            backend=default_backend()
        )
    
    return (
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ),
        public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
    )

PRIVATE_KEY, PUBLIC_KEY = load_rsa_keys()

# Security schemas
class TokenPayload(BaseModel):
    sub: str
    exp: int
    iat: int
    iss: str
    aud: str
    scopes: List[str] = []
    roles: List[str] = []

class UserSession(BaseModel):
    user_id: str
    session_id: str
    device_fingerprint: str
    last_activity: datetime

class SecurityConfig:
    TOKEN_ISSUER = os.getenv("TOKEN_ISSUER", "https://auth.example.com")
    TOKEN_AUDIENCE = os.getenv("TOKEN_AUDIENCE", "https://api.example.com")
    SESSION_TIMEOUT = timedelta(minutes=45)
    MAX_SESSIONS_PER_USER = 5

oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="/auth/token",
    scopes={
        "profile": "Read user profile information",
        "email": "Access email address",
        "admin": "Administrative privileges"
    }
)

class AuthHandler:
    def __init__(self):
        self.active_sessions: Dict[str, UserSession] = {}
        self.revoked_tokens = set()
        self.failed_attempts = {}
        self.lockout_threshold = 5
        self.lockout_duration = timedelta(minutes=15)

    @staticmethod
    def hash_password(password: str) -> str:
        salt = bcrypt.gensalt(rounds=12)
        return bcrypt.hashpw(password.encode(), salt).decode()

    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())

    def create_access_token(self, user_id: str, scopes: List[str], roles: List[str]) -> str:
        now = datetime.utcnow()
        expiration = now + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        
        payload = {
            "sub": user_id,
            "exp": expiration,
            "iat": now,
            "iss": SecurityConfig.TOKEN_ISSUER,
            "aud": SecurityConfig.TOKEN_AUDIENCE,
            "scopes": scopes,
            "roles": roles
        }
        
        return jwt.encode(
            payload,
            PRIVATE_KEY,
            algorithm=ALGORITHM,
            headers={"kid": os.getenv("KEY_ID")}
        )

    def create_refresh_token(self, user_id: str) -> str:
        expiration = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
        payload = {
            "sub": user_id,
            "exp": expiration,
            "iat": datetime.utcnow(),
            "iss": SecurityConfig.TOKEN_ISSUER,
            "aud": SecurityConfig.TOKEN_AUDIENCE,
            "token_type": "refresh"
        }
        
        return jwt.encode(
            payload,
            PRIVATE_KEY,
            algorithm=ALGORITHM,
            headers={"kid": os.getenv("KEY_ID")}
        )

    def decode_token(self, token: str) -> TokenPayload:
        try:
            payload = jwt.decode(
                token,
                PUBLIC_KEY,
                algorithms=[ALGORITHM],
                issuer=SecurityConfig.TOKEN_ISSUER,
                audience=SecurityConfig.TOKEN_AUDIENCE
            )
            return TokenPayload(**payload)
        except jwt.ExpiredSignatureError:
            logger.warning("Expired token attempt")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token expired",
                headers={"WWW-Authenticate": "Bearer"},
            )
        except jwt.InvalidTokenError as e:
            logger.error(f"Invalid token: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )

    def session_manager(self, user_id: str, device_fingerprint: str) -> str:
        session_id = os.urandom(16).hex()
        
        if user_id in self.active_sessions:
            if len(self.active_sessions[user_id]) >= SecurityConfig.MAX_SESSIONS_PER_USER:
                oldest_session = min(
                    self.active_sessions[user_id], 
                    key=lambda x: x.last_activity
                )
                del self.active_sessions[user_id][oldest_session.session_id]
        
        new_session = UserSession(
            user_id=user_id,
            session_id=session_id,
            device_fingerprint=device_fingerprint,
            last_activity=datetime.utcnow()
        )
        
        self.active_sessions.setdefault(user_id, {})[session_id] = new_session
        return session_id

    def refresh_session(self, user_id: str, session_id: str):
        if user_id in self.active_sessions and session_id in self.active_sessions[user_id]:
            self.active_sessions[user_id][session_id].last_activity = datetime.utcnow()

    def revoke_session(self, user_id: str, session_id: str):
        if user_id in self.active_sessions and session_id in self.active_sessions[user_id]:
            del self.active_sessions[user_id][session_id]
            self.revoked_tokens.add(session_id)

    def check_session(self, user_id: str, session_id: str) -> bool:
        session = self.active_sessions.get(user_id, {}).get(session_id)
        if not session:
            return False
            
        if datetime.utcnow() - session.last_activity > SecurityConfig.SESSION_TIMEOUT:
            self.revoke_session(user_id, session_id)
            return False
            
        return True

    def rate_limit_check(self, identifier: str) -> bool:
        attempts = self.failed_attempts.get(identifier, 0)
        if attempts >= self.lockout_threshold:
            last_attempt = self.failed_attempts.get(f"{identifier}_time")
            if last_attempt and (datetime.utcnow() - last_attempt) < self.lockout_duration:
                return False
            else:
                self.failed_attempts.pop(identifier, None)
                self.failed_attempts.pop(f"{identifier}_time", None)
        return True

    def auth_required(self, scopes: List[str] = None, roles: List[str] = None):
        def decorator(func):
            @wraps(func)
            async def wrapper(request: Request, token: str = Depends(oauth2_scheme), *args, **kwargs):
                if not self.rate_limit_check(request.client.host):
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail="Too many failed attempts"
                    )
                
                try:
                    payload = self.decode_token(token)
                    user_id = payload.sub
                    
                    # Validate session state
                    session_id = request.headers.get("X-Session-ID")
                    if not session_id or not self.check_session(user_id, session_id):
                        raise HTTPException(
                            status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Invalid session"
                        )
                    
                    # Scope validation
                    if scopes and not any(scope in payload.scopes for scope in scopes):
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail="Insufficient permissions"
                        )
                        
                    # Role validation
                    if roles and not any(role in payload.roles for role in roles):
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail="Insufficient privileges"
                        )
                    
                    # Update session activity
                    self.refresh_session(user_id, session_id)
                    
                    return await func(request, *args, **kwargs)
                
                except HTTPException:
                    self.failed_attempts[request.client.host] = self.failed_attempts.get(request.client.host, 0) + 1
                    self.failed_attempts[f"{request.client.host}_time"] = datetime.utcnow()
                    raise
                    
            return wrapper
        return decorator

# Example usage
auth_handler = AuthHandler()

# Example FastAPI endpoint
"""
@router.get("/protected")
@auth_handler.auth_required(scopes=["profile"], roles=["user"])
async def protected_route(request: Request):
    return {"message": "Access granted"}
"""
