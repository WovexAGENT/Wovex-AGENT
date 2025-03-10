import asyncio
import logging
import signal
import uuid
import psutil
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    TypeVar,
    Union,
    AsyncGenerator
)
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from prometheus_client import Counter, Gauge, Histogram

# Metrics Configuration
TASK_QUEUE_SIZE = Gauge('executor_task_queue_size', 'Current task queue size')
TASK_PROCESSING_TIME = Histogram(
    'executor_task_duration_seconds',
    'Task processing time distribution',
    ['task_type', 'status']
)
TASKS_COMPLETED = Counter(
    'executor_tasks_completed_total',
    'Total completed tasks',
    ['task_type', 'status']
)
ACTIVE_WORKERS = Gauge('executor_active_workers', 'Current active worker count')
SYSTEM_LOAD = Gauge('executor_system_load', 'Current system load average')
MEMORY_USAGE = Gauge('executor_memory_usage', 'Current memory usage percentage')

T = TypeVar('T')
R = TypeVar('R')

@dataclass
class ExecutorConfig:
    max_workers: int = 10
    max_memory_usage: float = 0.8
    max_cpu_usage: float = 0.75
    task_timeout: int = 300
    retry_count: int = 3
    retry_backoff: float = 1.5
    poll_interval: float = 0.1
    graceful_shutdown_timeout: int = 30
    enable_resource_limits: bool = True
    enable_metrics: bool = True

@dataclass
class TaskResult:
    task_id: str
    status: str
    result: Optional[Any] = None
    error: Optional[Exception] = None
    retries: int = 0
    execution_time: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

class BaseTask(ABC):
    def __init__(self):
        self.task_id = str(uuid.uuid4())
        self.created_at = datetime.utcnow()
        self.retry_count = 0
        self.idempotency_key: Optional[str] = None

    @abstractmethod
    async def execute(self) -> Any:
        pass

    @property
    @abstractmethod
    def task_type(self) -> str:
        pass

    def validate_input(self) -> bool:
        return True

    async def on_success(self, result: Any) -> None:
        pass

    async def on_failure(self, error: Exception) -> None:
        pass

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, reset_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure: Optional[datetime] = None
        self._open = False

    @property
    def is_open(self) -> bool:
        if self._open and self.last_failure:
            return (datetime.utcnow() - self.last_failure).total_seconds() < self.reset_timeout
        return self._open

    def record_failure(self):
        self.failure_count += 1
        self.last_failure = datetime.utcnow()
        if self.failure_count >= self.failure_threshold:
            self._open = True

    def reset(self):
        self.failure_count = 0
        self._open = False

class Executor:
    def __init__(self, config: ExecutorConfig = ExecutorConfig()):
        self.config = config
        self._queue = asyncio.Queue()
        self._workers: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        self._thread_pool = ThreadPoolExecutor()
        self._circuit_breaker = CircuitBreaker()
        self._resource_lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(config.max_workers)
        self.logger = logging.getLogger(self.__class__.__name__)

    async def initialize(self):
        self._setup_signal_handlers()
        self._start_workers()
        self.logger.info("Executor initialized with %d workers", self.config.max_workers)

    def _setup_signal_handlers(self):
        loop = asyncio.get_running_loop()
        for signame in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                signame,
                partial(self._handle_shutdown, signame)
            )

    def _start_workers(self):
        for _ in range(self.config.max_workers):
            worker = asyncio.create_task(self._worker_loop())
            self._workers.append(worker)

    async def submit(self, task: BaseTask) -> str:
        if self._shutdown_event.is_set():
            raise RuntimeError("Executor is shutting down")
        
        if not task.validate_input():
            raise ValueError("Invalid task input")
        
        await self._queue.put(task)
        TASK_QUEUE_SIZE.inc()
        return task.task_id

    async def _worker_loop(self):
        while not self._shutdown_event.is_set():
            try:
                task = await asyncio.wait_for(
                    self._queue.get(),
                    timeout=self.config.poll_interval
                )
                TASK_QUEUE_SIZE.dec()
                
                async with self._semaphore:
                    await self._process_task(task)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error("Worker error: %s", str(e))

    async def _process_task(self, task: BaseTask):
        start_time = datetime.utcnow()
        result = TaskResult(task_id=task.task_id, status="pending")
        
        try:
            if self._should_throttle():
                await self._apply_backpressure()
                
            await self._check_resource_limits()
            
            ACTIVE_WORKERS.inc()
            execution_time = await self._execute_task(task)
            
            result.status = "success"
            result.execution_time = execution_time
            await task.on_success(result.result)
            
            TASKS_COMPLETED.labels(
                task_type=task.task_type,
                status="success"
            ).inc()
            
        except Exception as e:
            result.status = "failed"
            result.error = e
            await task.on_failure(e)
            
            self._circuit_breaker.record_failure()
            await self._handle_retry(task)
            
            TASKS_COMPLETED.labels(
                task_type=task.task_type,
                status="failed"
            ).inc()
            
        finally:
            ACTIVE_WORKERS.dec()
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            TASK_PROCESSING_TIME.labels(
                task_type=task.task_type,
                status=result.status
            ).observe(duration)

    async def _execute_task(self, task: BaseTask) -> float:
        start_time = datetime.utcnow()
        try:
            if self.config.enable_metrics:
                with TASK_PROCESSING_TIME.labels(
                    task_type=task.task_type,
                    status="processing"
                ).time():
                    result.result = await asyncio.wait_for(
                        task.execute(),
                        timeout=self.config.task_timeout
                    )
        except asyncio.TimeoutError:
            raise TimeoutError(f"Task timeout after {self.config.task_timeout}s")
        finally:
            return (datetime.utcnow() - start_time).total_seconds()

    async def _handle_retry(self, task: BaseTask):
        if task.retry_count < self.config.retry_count:
            task.retry_count += 1
            backoff = self.config.retry_backoff ** task.retry_count
            self.logger.info("Retrying task %s in %.2fs", task.task_id, backoff)
            await asyncio.sleep(backoff)
            await self.submit(task)

    async def _check_resource_limits(self):
        if not self.config.enable_resource_limits:
            return
        
        async with self._resource_lock:
            memory = psutil.virtual_memory().percent / 100
            cpu = psutil.cpu_percent() / 100
            SYSTEM_LOAD.set(psutil.getloadavg()[0])
            MEMORY_USAGE.set(memory)
            
            if memory > self.config.max_memory_usage:
                raise ResourceWarning("Memory usage exceeds threshold")
            
            if cpu > self.config.max_cpu_usage:
                raise ResourceWarning("CPU usage exceeds threshold")

    def _should_throttle(self) -> bool:
        return (
            self._queue.qsize() > self.config.max_workers * 2
            or self._circuit_breaker.is_open
        )

    async def _apply_backpressure(self):
        throttle_delay = self._calculate_throttle_delay()
        self.logger.warning("Applying backpressure delay: %.2fs", throttle_delay)
        await asyncio.sleep(throttle_delay)

    def _calculate_throttle_delay(self) -> float:
        queue_size = self._queue.qsize()
        return min(2 ** (queue_size // 10), 60)

    async def _handle_shutdown(self, signame):
        self.logger.info("Received %s, initiating shutdown", signame)
        self._shutdown_event.set()
        
        await self._drain_queue()
        await self._stop_workers()
        await self._cleanup_resources()

    async def _drain_queue(self):
        while not self._queue.empty():
            task = await self._queue.get()
            self.logger.debug("Draining task %s", task.task_id)
            self._queue.task_done()

    async def _stop_workers(self):
        timeout = self.config.graceful_shutdown_timeout
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._workers),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            self.logger.warning("Forceful worker shutdown after timeout")

    async def _cleanup_resources(self):
        await self._thread_pool.shutdown(wait=True)
        self.logger.info("Executor shutdown complete")

    async def monitor_system(self):
        while not self._shutdown_event.is_set():
            memory = psutil.virtual_memory().percent
            cpu = psutil.cpu_percent()
            SYSTEM_LOAD.set(psutil.getloadavg()[0])
            MEMORY_USAGE.set(memory)
            await asyncio.sleep(5)

    async def __aenter__(self):
        await self.initialize()
        asyncio.create_task(self.monitor_system())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._handle_shutdown(signal.SIGTERM)

class ExampleTask(BaseTask):
    @property
    def task_type(self) -> str:
        return "example_task"

    async def execute(self) -> str:
        await asyncio.sleep(0.5)
        return "task_result"

async def main():
    config = ExecutorConfig(
        max_workers=5,
        task_timeout=10
    )
    
    async with Executor(config) as executor:
        tasks = [ExampleTask() for _ in range(20)]
        for task in tasks:
            await executor.submit(task)
        
        await asyncio.sleep(5)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
