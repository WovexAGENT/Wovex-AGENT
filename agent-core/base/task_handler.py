import asyncio
import logging
import heapq
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import (
    Any,
    Dict,
    Optional,
    List,
    Callable,
    Awaitable,
    TypeVar,
    Generic
)
from enum import Enum
from functools import total_ordering
import aiohttp
from pydantic import BaseModel, validator
from prometheus_client import Counter, Gauge, Histogram

# Metrics Configuration
TASK_QUEUE_SIZE = Gauge(
    'task_queue_size',
    'Current number of tasks in the queue'
)

TASK_PROCESSING_TIME = Histogram(
    'task_processing_seconds',
    'Time spent processing tasks',
    ['task_type', 'status']
)

TASK_RETRY_COUNTER = Counter(
    'task_retries_total',
    'Number of task retries',
    ['task_type']
)

TASK_FAILURE_COUNTER = Counter(
    'task_failures_total',
    'Number of failed tasks',
    ['task_type', 'reason']
)

class TaskStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"

class TaskPriority(int, Enum):
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4

@dataclass
class TaskConfiguration:
    max_concurrent_tasks: int = 10
    max_retries: int = 3
    retry_backoff_base: float = 2.0
    task_timeout: int = 300  # seconds
    priority_strategy: str = "strict"
    enable_duplicate_check: bool = True
    result_ttl: int = 3600  # 1 hour

@dataclass
class TaskResult:
    task_id: str
    status: TaskStatus
    result: Optional[Any] = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    execution_time: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

class TaskRequest(BaseModel):
    payload: Dict[str, Any]
    priority: TaskPriority = TaskPriority.NORMAL
    metadata: Dict[str, str] = {}
    max_retries: Optional[int] = None
    timeout: Optional[int] = None
    callback_url: Optional[str] = None

    @validator('priority')
    def validate_priority(cls, v):
        if not isinstance(v, TaskPriority):
            raise ValueError("Invalid priority value")
        return v

@total_ordering
class Task:
    def __init__(self, request: TaskRequest):
        self.id = str(uuid.uuid4())
        self.created_at = datetime.utcnow()
        self.priority = request.priority
        self.payload = request.payload
        self.metadata = request.metadata
        self.retry_count = 0
        self.max_retries = request.max_retries
        self.timeout = request.timeout
        self.callback_url = request.callback_url
        self.status = TaskStatus.PENDING
        self.result: Optional[TaskResult] = None
        self.lock = asyncio.Lock()

    def __lt__(self, other):
        if self.priority == other.priority:
            return self.created_at < other.created_at
        return self.priority.value > other.priority.value

class BaseTaskStorage(ABC):
    @abstractmethod
    async def store_task(self, task: Task):
        pass

    @abstractmethod
    async def retrieve_task(self, task_id: str) -> Optional[Task]:
        pass

    @abstractmethod
    async def update_task_status(self, task_id: str, status: TaskStatus):
        pass

class MemoryTaskStorage(BaseTaskStorage):
    def __init__(self):
        self.tasks: Dict[str, Task] = {}

    async def store_task(self, task: Task):
        self.tasks[task.id] = task

    async def retrieve_task(self, task_id: str) -> Optional[Task]:
        return self.tasks.get(task_id)

    async def update_task_status(self, task_id: str, status: TaskStatus):
        if task := self.tasks.get(task_id):
            task.status = status

class TaskHandler:
    def __init__(
        self,
        config: TaskConfiguration,
        storage: BaseTaskStorage = None,
        worker_pool_size: int = 4
    ):
        self.config = config
        self.storage = storage or MemoryTaskStorage()
        self.worker_pool_size = worker_pool_size
        self.task_queue = asyncio.PriorityQueue()
        self.active_tasks = set()
        self.workers = []
        self.logger = logging.getLogger(self.__class__.__name__)
        self._shutdown = False
        self._semaphore = asyncio.Semaphore(config.max_concurrent_tasks)
        self._setup_metrics()

    def _setup_metrics(self):
        TASK_QUEUE_SIZE.set_function(lambda: self.task_queue.qsize())

    async def initialize(self):
        """Start task processing workers"""
        self.workers = [
            asyncio.create_task(self._worker_loop())
            for _ in range(self.worker_pool_size)
        ]
        self.logger.info("Task handler initialized")

    async def enqueue_task(self, request: TaskRequest) -> str:
        """Add a new task to the processing queue"""
        task = Task(request)
        async with task.lock:
            await self.storage.store_task(task)
            await self._add_to_queue(task)
            self.logger.info(f"Enqueued task {task.id}")
            TASK_QUEUE_SIZE.inc()
            return task.id

    async def _add_to_queue(self, task: Task):
        """Internal method to add task to priority queue"""
        heapq.heappush(self.task_queue._queue, task)
        self.task_queue._unfinished_tasks += 1
        self.task_queue._finished.clear()

    async def _worker_loop(self):
        """Main worker processing loop"""
        while not self._shutdown:
            try:
                task = await self.task_queue.get()
                async with self._semaphore:
                    await self._process_task(task)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Worker error: {str(e)}")

    async def _process_task(self, task: Task):
        """Process individual task with retry logic"""
        async with task.lock:
            task.status = TaskStatus.PROCESSING
            await self.storage.update_task_status(task.id, task.status)

        start_time = datetime.utcnow()
        try:
            async with task.lock:
                result = await self._execute_with_timeout(task)
                processing_time = (datetime.utcnow() - start_time).total_seconds()

            task.result = TaskResult(
                task_id=task.id,
                status=TaskStatus.COMPLETED,
                result=result,
                execution_time=processing_time
            )
            TASK_PROCESSING_TIME.labels(
                task_type=task.metadata.get('type', 'unknown'),
                status='success'
            ).observe(processing_time)

        except Exception as e:
            async with task.lock:
                await self._handle_task_failure(task, e, start_time)
                if task.retry_count < (task.max_retries or self.config.max_retries):
                    await self._schedule_retry(task)
                    return

                task.result = TaskResult(
                    task_id=task.id,
                    status=TaskStatus.FAILED,
                    error=str(e),
                    execution_time=(datetime.utcnow() - start_time).total_seconds()
                )
                TASK_FAILURE_COUNTER.labels(
                    task_type=task.metadata.get('type', 'unknown'),
                    reason=str(e.__class__.__name__)
                ).inc()

        finally:
            async with task.lock:
                await self._finalize_task_processing(task)
                TASK_QUEUE_SIZE.dec()

    async def _execute_with_timeout(self, task: Task) -> Any:
        """Execute task with timeout control"""
        timeout = task.timeout or self.config.task_timeout
        try:
            return await asyncio.wait_for(
                self.execute_task(task.payload),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            raise TimeoutError(f"Task timed out after {timeout} seconds")

    async def _handle_task_failure(self, task: Task, error: Exception, start_time: datetime):
        """Handle task execution failure"""
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        task.result = TaskResult(
            task_id=task.id,
            status=TaskStatus.FAILED,
            error=str(error),
            execution_time=processing_time
        )
        TASK_PROCESSING_TIME.labels(
            task_type=task.metadata.get('type', 'unknown'),
            status='failure'
        ).observe(processing_time)

    async def _schedule_retry(self, task: Task):
        """Reschedule task for retry with backoff"""
        async with task.lock:
            task.retry_count += 1
            backoff = self.config.retry_backoff_base ** task.retry_count
            task.status = TaskStatus.RETRYING
            await self.storage.update_task_status(task.id, task.status)
            TASK_RETRY_COUNTER.labels(
                task_type=task.metadata.get('type', 'unknown')
            ).inc()

        self.logger.warning(
            f"Scheduling retry {task.retry_count} for task {task.id} in {backoff}s"
        )
        await asyncio.sleep(backoff)
        await self.enqueue_task(TaskRequest(**task.__dict__))

    async def _finalize_task_processing(self, task: Task):
        """Finalize task processing steps"""
        async with task.lock:
            await self.storage.update_task_status(task.id, task.status)
            if task.callback_url:
                await self._send_callback(task)
            self.task_queue.task_done()

    async def _send_callback(self, task: Task):
        """Send task result to callback URL"""
        async with aiohttp.ClientSession() as session:
            try:
                await session.post(
                    task.callback_url,
                    json=task.result.dict(),
                    timeout=5
                )
            except Exception as e:
                self.logger.error(f"Callback failed for task {task.id}: {str(e)}")

    @abstractmethod
    async def execute_task(self, payload: Dict[str, Any]) -> Any:
        """Abstract method to be implemented by concrete handlers"""
        pass

    async def graceful_shutdown(self):
        """Initiate graceful shutdown of task handler"""
        self._shutdown = True
        self.logger.info("Starting graceful shutdown...")
        
        # Cancel worker tasks
        for worker in self.workers:
            worker.cancel()
        
        # Wait for current tasks to complete
        await self.task_queue.join()
        self.logger.info("Shutdown completed")

# Example Implementation
class ExampleTaskHandler(TaskHandler):
    async def execute_task(self, payload: Dict[str, Any]) -> Any:
        """Concrete task execution logic"""
        # Implement actual task processing here
        await asyncio.sleep(1)  # Simulate work
        return {"processed": True}

# Decorators
def task_metrics(func):
    async def wrapper(self, *args, **kwargs):
        start_time = datetime.utcnow()
        try:
            result = await func(self, *args, **kwargs)
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            TASK_PROCESSING_TIME.labels(
                task_type=kwargs.get('task_type', 'unknown'),
                status='success'
            ).observe(processing_time)
            return result
        except Exception as e:
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            TASK_PROCESSING_TIME.labels(
                task_type=kwargs.get('task_type', 'unknown'),
                status='failure'
            ).observe(processing_time)
            raise
    return wrapper

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    async def main():
        config = TaskConfiguration(
            max_concurrent_tasks=5,
            max_retries=3
        )
        
        handler = ExampleTaskHandler(config)
        await handler.initialize()
        
        # Example task submission
        task_request = TaskRequest(
            payload={"data": "example"},
            priority=TaskPriority.HIGH,
            metadata={"type": "demo"}
        )
        
        task_id = await handler.enqueue_task(task_request)
        print(f"Submitted task: {task_id}")
        
        # Allow time for processing
        await asyncio.sleep(2)
        await handler.graceful_shutdown()
    
    asyncio.run(main())
