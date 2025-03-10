import asyncio
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, List, AsyncGenerator
from pydantic import BaseModel, BaseSettings, AnyUrl, Field
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import aiohttp
from prometheus_client import Counter, Histogram, Gauge

# ======================
# Configuration Models
# ======================

class ClientSettings(BaseSettings):
    api_base_url: AnyUrl = Field(..., env="API_BASE_URL")
    auth_token: str = Field(..., env="API_AUTH_TOKEN")
    max_retries: int = 5
    request_timeout: int = 30
    heartbeat_interval: int = 300
    metrics_port: int = 9091

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# ======================
# Data Models
# ======================

class NodeRegistration(BaseModel):
    node_id: uuid.UUID
    cluster: str
    role: str
    capabilities: Dict[str, Any]
    labels: Dict[str, str] = {}

class TaskRequest(BaseModel):
    parameters: Dict[str, Any]
    priority: int = 0
    timeout: timedelta = timedelta(minutes=30)

class TaskStatus(BaseModel):
    task_id: uuid.UUID
    status: str
    created_at: datetime
    updated_at: datetime
    result: Optional[Dict[str, Any]]

# ======================
# Metrics
# ======================

REQUEST_COUNTER = Counter(
    'client_requests_total',
    'Total API requests made',
    ['method', 'endpoint', 'status']
)

REQUEST_LATENCY = Histogram(
    'client_request_latency_seconds',
    'API request latency histogram',
    ['method', 'endpoint']
)

HEARTBEAT_GAUGE = Gauge(
    'client_heartbeat_status',
    'Client heartbeat status (1=healthy, 0=unhealthy)'
)

# ======================
# Client Implementation
# ======================

class APIClient:
    def __init__(self, settings: ClientSettings):
        self.settings = settings
        self.session = aiohttp.ClientSession(
            base_url=str(settings.api_base_url),
            headers={
                "User-Agent": "DistributedSystemClient/1.0",
                "Accept": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=settings.request_timeout)
        )
        self._auth_token = settings.auth_token
        self._node_id = uuid.uuid4()
        self._heartbeat_task = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def start(self):
        await self.register_node()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def close(self):
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        await self.session.close()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=10),
        retry=retry_if_exception_type(aiohttp.ClientError)
    )
    async def _request(self, method: str, endpoint: str, **kwargs) -> Any:
        headers = kwargs.pop('headers', {})
        headers["Authorization"] = f"Bearer {self._auth_token}"

        start_time = time.monotonic()
        try:
            async with self.session.request(
                method, endpoint, headers=headers, **kwargs
            ) as response:
                latency = time.monotonic() - start_time
                REQUEST_LATENCY.labels(method, endpoint).observe(latency)
                
                if response.status >= 400:
                    REQUEST_COUNTER.labels(method, endpoint, response.status).inc()
                    response.raise_for_status()

                if response.content_type == 'application/json':
                    return await response.json()
                return await response.text()
        except aiohttp.ClientError as e:
            REQUEST_COUNTER.labels(method, endpoint, "error").inc()
            logging.error(f"Request failed: {str(e)}")
            raise

    async def register_node(self) -> NodeRegistration:
        registration = NodeRegistration(
            node_id=self._node_id,
            cluster="default",
            role="worker",
            capabilities={"compute": True, "storage": 100}
        )
        await self._request(
            "POST", "/nodes/register",
            json=registration.dict()
        )
        return registration

    async def _heartbeat_loop(self):
        while True:
            try:
                await self.send_heartbeat()
                HEARTBEAT_GAUGE.set(1)
                await asyncio.sleep(self.settings.heartbeat_interval)
            except Exception as e:
                HEARTBEAT_GAUGE.set(0)
                logging.error(f"Heartbeat failed: {str(e)}")
                await asyncio.sleep(10)

    async def send_heartbeat(self) -> Dict[str, Any]:
        return await self._request(
            "POST", f"/nodes/{self._node_id}/heartbeat"
        )

    async def create_task(self, task: TaskRequest) -> TaskStatus:
        response = await self._request(
            "POST", "/tasks",
            json=task.dict()
        )
        return TaskStatus(**response)

    async def get_task_status(self, task_id: uuid.UUID) -> TaskStatus:
        response = await self._request(
            "GET", f"/tasks/{task_id}"
        )
        return TaskStatus(**response)

    async def stream_task_updates(self) -> AsyncGenerator[TaskStatus, None]:
        async with self.session.ws_connect("/tasks/updates") as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    yield TaskStatus(**msg.json())
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break

# ======================
# Command Line Interface
# ======================

async def main():
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Distributed System Client")
    parser.add_argument("--register", action="store_true", help="Register new node")
    parser.add_argument("--create-task", type=str, help="Create new task with JSON parameters")
    args = parser.parse_args()

    settings = ClientSettings()
    async with APIClient(settings) as client:
        if args.register:
            await client.register_node()
            print("Node registration successful")
        
        if args.create_task:
            task_data = json.loads(args.create_task)
            task = TaskRequest(**task_data)
            result = await client.create_task(task)
            print(f"Created task: {result.task_id}")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    asyncio.run(main())
