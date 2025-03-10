import asyncio
import logging
import signal
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any, AsyncGenerator

import asyncpg
from fastapi import FastAPI, HTTPException, status, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, BaseSettings, AnyUrl, ValidationError
from prometheus_client import make_asgi_app, Counter, Gauge, Histogram

# ======================
# Configuration
# ======================

class Settings(BaseSettings):
    app_name: str = "Hadean Coordinator"
    environment: str = "production"
    database_url: AnyUrl
    jwt_secret: str
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    node_heartbeat_timeout: int = 300  # 5 minutes
    max_concurrent_tasks: int = 1000
    prometheus_port: int = 9090
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# ======================
# Database Models
# ======================

class NodeRegistration(BaseModel):
    node_id: uuid.UUID
    cluster: str
    role: str
    capabilities: Dict[str, Any]
    labels: Dict[str, str]
    last_heartbeat: datetime

class TaskRecord(BaseModel):
    task_id: uuid.UUID
    owner: str
    status: str
    created_at: datetime
    updated_at: datetime
    parameters: Dict[str, Any]
    result: Optional[Dict[str, Any]]

# ======================
# Security
# ======================

security = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def create_access_token(data: dict, expires_delta: timedelta) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.jwt_secret, algorithm=settings.jwt_algorithm)

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict[str, Any]:
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm]
        )
        return payload
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

# ======================
# Metrics
# ======================

REQUEST_COUNT = Counter(
    'http_requests_total',
    'Total HTTP Requests',
    ['method', 'endpoint', 'http_status']
)

REQUEST_LATENCY = Histogram(
    'http_request_latency_seconds',
    'HTTP request latency',
    ['method', 'endpoint']
)

ACTIVE_TASKS = Gauge(
    'active_tasks_total',
    'Current number of active tasks'
)

NODE_HEALTH = Gauge(
    'node_health_status',
    'Node health status (1=healthy, 0=unhealthy)',
    ['node_id', 'cluster']
)

# ======================
# Application Setup
# ======================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize database connection pool
    app.state.db_pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=5,
        max_size=20
    )
    
    # Start background tasks
    app.state.background_tasks = set()
    task = asyncio.create_task(node_health_monitor())
    app.state.background_tasks.add(task)
    task.add_done_callback(app.state.background_tasks.discard)
    
    yield  # Application runs here
    
    # Cleanup resources
    await app.state.db_pool.close()
    for task in app.state.background_tasks:
        task.cancel()

settings = Settings()
app = FastAPI(lifespan=lifespan)
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# ======================
# Middleware
# ======================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def monitor_requests(request: Request, call_next):
    start_time = datetime.utcnow()
    method = request.method
    endpoint = request.url.path
    
    try:
        response = await call_next(request)
        elapsed = (datetime.utcnow() - start_time).total_seconds()
        
        REQUEST_COUNT.labels(method, endpoint, response.status_code).inc()
        REQUEST_LATENCY.labels(method, endpoint).observe(elapsed)
        
        return response
    except Exception as e:
        elapsed = (datetime.utcnow() - start_time).total_seconds()
        REQUEST_COUNT.labels(method, endpoint, 500).inc()
        REQUEST_LATENCY.labels(method, endpoint).observe(elapsed)
        raise

# ======================
# Database Utilities
# ======================

async def execute_query(query: str, *args) -> List[asyncpg.Record]:
    async with app.state.db_pool.acquire() as conn:
        return await conn.fetch(query, *args)

async def execute_command(query: str, *args) -> str:
    async with app.state.db_pool.acquire() as conn:
        return await conn.execute(query, *args)

# ======================
# Background Tasks
# ======================

async def node_health_monitor():
    while True:
        try:
            query = """
                UPDATE nodes 
                SET status = 'unhealthy'
                WHERE last_heartbeat < NOW() - INTERVAL '%s SECONDS'
                RETURNING node_id
            """
            stale_nodes = await execute_query(query, settings.node_heartbeat_timeout)
            
            for node in stale_nodes:
                logging.warning(f"Marked node {node['node_id']} as unhealthy")
                NODE_HEALTH.labels(node_id=node['node_id'], cluster="default").set(0)
            
            await asyncio.sleep(60)
        except Exception as e:
            logging.error(f"Node health monitor error: {str(e)}")
            await asyncio.sleep(10)

# ======================
# API Endpoints
# ======================

@app.post("/nodes/register", status_code=status.HTTP_201_CREATED)
async def register_node(node: NodeRegistration, user: dict = Depends(get_current_user)):
    try:
        query = """
            INSERT INTO nodes (node_id, cluster, role, capabilities, labels, last_heartbeat)
            VALUES (\$1, \$2, \$3, \$4, \$5, \$6)
            ON CONFLICT (node_id) DO UPDATE SET
                cluster = EXCLUDED.cluster,
                role = EXCLUDED.role,
                capabilities = EXCLUDED.capabilities,
                labels = EXCLUDED.labels,
                last_heartbeat = EXCLUDED.last_heartbeat
            RETURNING node_id
        """
        result = await execute_query(
            query,
            str(node.node_id),
            node.cluster,
            node.role,
            node.capabilities,
            node.labels,
            node.last_heartbeat
        )
        
        NODE_HEALTH.labels(node_id=node.node_id, cluster=node.cluster).set(1)
        return {"node_id": result[0]["node_id"]}
    except asyncpg.PostgresError as e:
        logging.error(f"Database error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database operation failed"
        )

@app.post("/nodes/{node_id}/heartbeat")
async def receive_heartbeat(node_id: uuid.UUID):
    try:
        query = """
            UPDATE nodes 
            SET last_heartbeat = NOW() 
            WHERE node_id = \$1
        """
        await execute_command(query, str(node_id))
        NODE_HEALTH.labels(node_id=node_id, cluster="default").set(1)
        return {"status": "acknowledged"}
    except asyncpg.PostgresError as e:
        logging.error(f"Heartbeat update failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to update heartbeat"
        )

@app.post("/tasks", status_code=status.HTTP_202_ACCEPTED)
async def create_task(task: Dict[str, Any], user: dict = Depends(get_current_user)):
    try:
        task_id = uuid.uuid4()
        query = """
            INSERT INTO tasks 
            (task_id, owner, status, parameters, created_at, updated_at)
            VALUES (\$1, \$2, 'pending', \$3, NOW(), NOW())
        """
        await execute_command(
            query,
            str(task_id),
            user["sub"],
            task
        )
        
        ACTIVE_TASKS.inc()
        return {"task_id": task_id}
    except asyncpg.UniqueViolationError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Task already exists"
        )

@app.get("/health", include_in_schema=False)
async def health_check():
    try:
        await execute_query("SELECT 1")
        return {"status": "healthy"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service unavailable"
        )

# ======================
# Signal Handling
# ======================

def handle_exit(sig, frame):
    logging.info("Received shutdown signal")
    loop = asyncio.get_event_loop()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    
    for task in tasks:
        task.cancel()
    
    loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
    loop.stop()

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

# ======================
# Main Execution
# ======================

if __name__ == "__main__":
    import uvicorn
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_config=None,
        access_log=False,
        timeout_keep_alive=60
    )
