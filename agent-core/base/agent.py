import asyncio
import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import wraps
import aiohttp
from pydantic import BaseModel, ValidationError
import numpy as np
from prometheus_client import Counter, Gauge, Histogram

# Metrics Configuration
AGENT_REQUEST_COUNTER = Counter(
    'agent_requests_total',
    'Total number of agent requests',
    ['method', 'endpoint', 'status']
)

AGENT_PROCESSING_TIME = Histogram(
    'agent_processing_seconds',
    'Time spent processing agent tasks',
    ['task_type']
)

AGENT_MEMORY_USAGE = Gauge(
    'agent_memory_usage_bytes',
    'Current memory usage of the agent'
)

@dataclass
class AgentConfiguration:
    name: str
    version: str = "1.0.0"
    max_concurrent_tasks: int = 10
    heartbeat_interval: int = 30
    retry_policy: Dict[str, Any] = field(default_factory=lambda: {
        "max_attempts": 3,
        "backoff_factor": 1.5
    })
    api_timeout: int = 30
    logging_level: str = "INFO"

class TaskPayload(BaseModel):
    task_id: str
    parameters: Dict[str, Any]
    metadata: Dict[str, str] = {}
    priority: int = 1

class AgentMessage(BaseModel):
    sender: str
    recipient: str
    content: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    message_type: str = "default"

class AgentBase(ABC):
    def __init__(self, config: AgentConfiguration):
        self.config = config
        self._setup_logging()
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.api_timeout)
        )
        self._task_queue = asyncio.Queue()
        self._active_tasks = set()
        self._shutdown_event = asyncio.Event()
        self._plugins = []
        self._state = {}

    def _setup_logging(self):
        logging.basicConfig(
            level=self.config.logging_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.config.name)

    async def initialize(self):
        """Initialize agent resources and connections"""
        self.logger.info(f"Initializing agent {self.config.name}")
        await self._start_background_tasks()
        await self._load_plugins()
        self.logger.info("Agent initialization complete")

    async def _start_background_tasks(self):
        """Start essential background tasks"""
        self._heartbeat_task = asyncio.create_task(self._send_heartbeat())
        self._task_processor = asyncio.create_task(self._process_tasks())

    async def _load_plugins(self):
        """Load and initialize agent plugins"""
        # Plugin loading implementation here
        pass

    @AGENT_PROCESSING_TIME.time()
    async def process_task(self, task: TaskPayload) -> Dict[str, Any]:
        """Main task processing method"""
        self.logger.debug(f"Processing task {task.task_id}")
        try:
            result = await self._execute_task(task)
            return {"status": "success", "result": result}
        except Exception as e:
            self.logger.error(f"Task failed: {str(e)}")
            return {"status": "error", "message": str(e)}

    @abstractmethod
    async def _execute_task(self, task: TaskPayload) -> Any:
        """Abstract method for task execution logic"""
        pass

    async def send_message(self, message: AgentMessage):
        """Send message to other agents"""
        headers = {
            "Content-Type": "application/json",
            "X-Agent-Name": self.config.name
        }
        try:
            async with self._session.post(
                f"http://{message.recipient}/messages",
                json=message.dict(),
                headers=headers
            ) as response:
                AGENT_REQUEST_COUNTER.labels(
                    method="POST",
                    endpoint="/messages",
                    status=response.status
                ).inc()
                response.raise_for_status()
        except aiohttp.ClientError as e:
            self.logger.error(f"Message sending failed: {str(e)}")

    async def receive_message(self, message: AgentMessage):
        """Handle incoming messages"""
        self.logger.info(f"Received message from {message.sender}")
        # Message handling logic here

    async def _send_heartbeat(self):
        """Periodic heartbeat signaling"""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.heartbeat_interval)
                self.logger.debug("Sending heartbeat")
                # Actual heartbeat implementation
            except asyncio.CancelledError:
                break

    async def _process_tasks(self):
        """Process tasks from the queue"""
        while not self._shutdown_event.is_set():
            task = await self._task_queue.get()
            task_future = asyncio.create_task(
                self._handle_task_with_retry(task)
            )
            self._active_tasks.add(task_future)
            task_future.add_done_callback(
                lambda f: self._active_tasks.remove(f)
            )

    async def _handle_task_with_retry(self, task: TaskPayload):
        """Handle task execution with retry logic"""
        max_attempts = self.config.retry_policy["max_attempts"]
        backoff_factor = self.config.retry_policy["backoff_factor"]

        for attempt in range(max_attempts):
            try:
                return await self.process_task(task)
            except Exception as e:
                if attempt == max_attempts - 1:
                    raise
                delay = backoff_factor ** attempt
                self.logger.warning(
                    f"Task failed, retrying in {delay} seconds: {str(e)}"
                )
                await asyncio.sleep(delay)

    def add_plugin(self, plugin):
        """Add plugin to the agent"""
        self._plugins.append(plugin)
        self.logger.info(f"Added plugin: {plugin.__class__.__name__}")

    async def graceful_shutdown(self):
        """Shutdown agent gracefully"""
        self.logger.info("Initiating shutdown sequence")
        self._shutdown_event.set()
        
        # Cancel background tasks
        self._heartbeat_task.cancel()
        self._task_processor.cancel()
        
        # Wait for active tasks to complete
        await asyncio.gather(*self._active_tasks, return_exceptions=True)
        
        # Close network connections
        await self._session.close()
        self.logger.info("Shutdown completed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        asyncio.run(self.graceful_shutdown())

# Decorators
def validate_payload(schema: BaseModel):
    def decorator(func):
        @wraps(func)
        async def wrapper(self, payload: Dict[str, Any], *args, **kwargs):
            try:
                validated = schema(**payload)
                return await func(self, validated, *args, **kwargs)
            except ValidationError as e:
                self.logger.error(f"Validation error: {str(e)}")
                raise
        return wrapper
    return decorator

def rate_limited(max_calls: int, period: int):
    def decorator(func):
        calls = []
        
        @wraps(func)
        async def wrapper(*args, **kwargs):
            nonlocal calls
            now = datetime.now()
            
            # Remove outdated timestamps
            calls = [t for t in calls if t > now - timedelta(seconds=period)]
            
            if len(calls) >= max_calls:
                raise Exception("Rate limit exceeded")
                
            calls.append(now)
            return await func(*args, **kwargs)
        return wrapper
    return decorator

# Example Plugin System
class AgentPlugin(ABC):
    @abstractmethod
    async def initialize(self, agent: AgentBase):
        pass
    
    @abstractmethod
    async def cleanup(self):
        pass

class MonitoringPlugin(AgentPlugin):
    async def initialize(self, agent: AgentBase):
        agent.add_route('/metrics', self.metrics_endpoint)
        
    async def metrics_endpoint(self, request):
        # Implement metrics collection
        pass

# Example Implementation
class ConcreteAgent(AgentBase):
    def __init__(self, config: AgentConfiguration):
        super().__init__(config)
        self._model = None

    async def _load_model(self):
        # Model loading implementation
        pass

    async def _execute_task(self, task: TaskPayload) -> Any:
        # Task-specific execution logic
        return {"result": "processed"}

# API Endpoints
class AgentAPIServer:
    def __init__(self, agent: AgentBase):
        self.agent = agent
        self.app = web.Application()
        self._setup_routes()

    def _setup_routes(self):
        self.app.router.add_post('/tasks', self.handle_task)
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_post('/messages', self.receive_message)

    async def handle_task(self, request):
        AGENT_REQUEST_COUNTER.labels(
            method="POST",
            endpoint="/tasks",
            status="200"
        ).inc()
        
        try:
            data = await request.json()
            task = TaskPayload(**data)
            result = await self.agent.process_task(task)
            return web.json_response(result)
        except ValidationError as e:
            return web.json_response(
                {"error": str(e)},
                status=400
            )

    async def health_check(self, request):
        return web.json_response({
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat()
        })

    async def receive_message(self, request):
        data = await request.json()
        message = AgentMessage(**data)
        await self.agent.receive_message(message)
        return web.Response(status=200)

if __name__ == "__main__":
    config = AgentConfiguration(
        name="hadean-core-agent",
        max_concurrent_tasks=20,
        logging_level="DEBUG"
    )
    
    agent = ConcreteAgent(config)
    server = AgentAPIServer(agent)
    
    web.run_app(server.app, port=8080)
