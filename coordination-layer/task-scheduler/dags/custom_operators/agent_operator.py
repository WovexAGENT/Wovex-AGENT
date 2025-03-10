import asyncio
import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import (
    Any,
    Awaitable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    AsyncGenerator,
    Callable,
    Coroutine
)
import aiohttp
import psutil
from pydantic import BaseModel, ValidationError
from prometheus_client import Counter, Gauge, Histogram

# Monitoring Metrics
AGENT_START_COUNTER = Counter('agent_start_operations', 'Agent startup attempts', ['agent_type'])
AGENT_STOP_COUNTER = Counter('agent_stop_operations', 'Agent shutdown attempts', ['status'])
AGENT_HEALTH_GAUGE = Gauge('agent_health_status', 'Agent health status', ['agent_id'])
OPERATION_DURATION = Histogram('agent_operation_duration', 'Operation timing metrics', ['operation'])

# Custom Exceptions
class AgentError(Exception):
    pass

class AgentTimeoutError(AgentError):
    pass

class AgentConfigError(AgentError):
    pass

class AgentCommunicationError(AgentError):
    pass

class AgentStateError(AgentError):
    pass

# State Management
class AgentState(Enum):
    INITIALIZED = "initialized"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"

class HealthStatus(Enum):
    HEALTHY = 1
    DEGRADED = 2
    UNHEALTHY = 3

@dataclass
class ResourceUsage:
    cpu_percent: float
    memory_mb: float
    network_tx: float
    network_rx: float

@dataclass
class AgentStatus:
    state: AgentState
    health: HealthStatus
    last_heartbeat: datetime
    resource_usage: ResourceUsage
    active_tasks: int

# Configuration Models
class AgentConfig(BaseModel):
    id: str
    type: str
    startup_timeout: int = 30
    shutdown_timeout: int = 15
    max_retries: int = 3
    heartbeat_interval: int = 60
    api_endpoint: Optional[str]
    monitoring_interval: int = 5

class AgentOperator:
    def __init__(self, config: AgentConfig):
        self.config = config
        self._state: AgentState = AgentState.INITIALIZED
        self._status = AgentStatus(
            state=self._state,
            health=HealthStatus.UNHEALTHY,
            last_heartbeat=datetime.utcnow(),
            resource_usage=ResourceUsage(0.0, 0.0, 0.0, 0.0),
            active_tasks=0
        )
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self._monitor_task: Optional[asyncio.Task] = None
        self._logger = logging.getLogger(f"AgentOperator.{self.config.id}")
        self._callbacks: Dict[str, List[Callable]] = {
            'pre_start': [],
            'post_start': [],
            'pre_stop': [],
            'post_stop': []
        }

    def register_callback(self, event: str, callback: Callable[[Dict[str, Any]], Coroutine]):
        if event not in self._callbacks:
            raise ValueError(f"Invalid event type: {event}")
        self._callbacks[event].append(callback)

    async def _execute_callbacks(self, event: str, context: Dict[str, Any]):
        for callback in self._callbacks[event]:
            try:
                await callback(context)
            except Exception as e:
                self._logger.error(f"Callback error for {event}: {str(e)}")

    async def _update_state(self, new_state: AgentState):
        async with self._lock:
            self._state = new_state
            self._status.state = new_state
            self._logger.info(f"State transition to {new_state.value}")

    async def _health_check(self) -> HealthStatus:
        try:
            if self.config.api_endpoint:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{self.config.api_endpoint}/health") as response:
                        if response.status == 200:
                            return HealthStatus.HEALTHY
                        return HealthStatus.UNHEALTHY
            else:
                return HealthStatus.HEALTHY if self._state == AgentState.RUNNING else HealthStatus.UNHEALTHY
        except Exception as e:
            self._logger.warning(f"Health check failed: {str(e)}")
            return HealthStatus.UNHEALTHY

    async def _resource_monitor(self):
        process = psutil.Process()
        net_io = psutil.net_io_counters()
        while self._state in [AgentState.STARTING, AgentState.RUNNING]:
            try:
                cpu = process.cpu_percent()
                mem = process.memory_info().rss / 1024 / 1024
                new_net_io = psutil.net_io_counters()
                
                self._status.resource_usage = ResourceUsage(
                    cpu_percent=cpu,
                    memory_mb=mem,
                    network_tx=(new_net_io.bytes_sent - net_io.bytes_sent) / 1024,
                    network_rx=(new_net_io.bytes_recv - net_io.bytes_recv) / 1024
                )
                net_io = new_net_io
                
                self._status.health = await self._health_check()
                self._status.last_heartbeat = datetime.utcnow()
                
                AGENT_HEALTH_GAUGE.labels(agent_id=self.config.id).set(self._status.health.value)
                
                await asyncio.sleep(self.config.monitoring_interval)
            except Exception as e:
                self._logger.error(f"Monitoring error: {str(e)}")
                await asyncio.sleep(5)

    @OPERATION_DURATION.time()
    async def start(self) -> AgentStatus:
        AGENT_START_COUNTER.labels(agent_type=self.config.type).inc()
        
        if self._state != AgentState.INITIALIZED and self._state != AgentState.STOPPED:
            raise AgentStateError(f"Cannot start from {self._state.value} state")

        await self._execute_callbacks('pre_start', {'config': self.config})
        
        try:
            await self._update_state(AgentState.STARTING)
            self._monitor_task = asyncio.create_task(self._resource_monitor())
            
            # Simulated startup process
            await asyncio.sleep(2)
            
            if self.config.api_endpoint:
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=self.config.startup_timeout)
                )
                async with self._session.get(f"{self.config.api_endpoint}/initialize") as resp:
                    if resp.status != 200:
                        raise AgentCommunicationError("Initialization failed")

            await self._update_state(AgentState.RUNNING)
            self._logger.info(f"Agent {self.config.id} started successfully")
            
            await self._execute_callbacks('post_start', {'status': self._status})
            return self._status
            
        except Exception as e:
            await self._update_state(AgentState.ERROR)
            self._logger.error(f"Startup failed: {str(e)}")
            raise AgentError("Agent startup failed") from e

    @OPERATION_DURATION.time()
    async def stop(self) -> AgentStatus:
        AGENT_STOP_COUNTER.labels(status=self._state.value).inc()
        
        if self._state not in [AgentState.RUNNING, AgentState.ERROR]:
            raise AgentStateError(f"Cannot stop from {self._state.value} state")

        await self._execute_callbacks('pre_stop', {'status': self._status})
        
        try:
            await self._update_state(AgentState.STOPPING)
            
            if self._monitor_task:
                self._monitor_task.cancel()
                try:
                    await self._monitor_task
                except asyncio.CancelledError:
                    pass

            if self._session:
                await self._session.close()

            # Simulated shutdown process
            await asyncio.sleep(1)
            
            await self._update_state(AgentState.STOPPED)
            self._logger.info(f"Agent {self.config.id} stopped successfully")
            
            await self._execute_callbacks('post_stop', {'status': self._status})
            return self._status
            
        except Exception as e:
            await self._update_state(AgentState.ERROR)
            self._logger.error(f"Shutdown failed: {str(e)}")
            raise AgentError("Agent shutdown failed") from e

    async def restart(self) -> AgentStatus:
        try:
            await self.stop()
            await asyncio.sleep(1)
            return await self.start()
        except Exception as e:
            raise AgentError("Restart failed") from e

    async def get_status(self) -> AgentStatus:
        return self._status

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

async def example_usage():
    config = AgentConfig(
        id="ai-agent-1",
        type="cognitive",
        api_endpoint="http://agent-api:8080"
    )
    
    async def startup_notification(context):
        print(f"Starting agent: {context['config'].id}")
    
    try:
        async with AgentOperator(config) as operator:
            operator.register_callback('pre_start', startup_notification)
            
            status = await operator.get_status()
            print(f"Agent status: {status.state.value}")
            
            await asyncio.sleep(10)
            
    except AgentError as e:
        print(f"Agent operation failed: {str(e)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(example_usage())
