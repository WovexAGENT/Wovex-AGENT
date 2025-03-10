import asyncio
import logging
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
    Set,
    Tuple,
    TypeVar,
    Union,
    AsyncGenerator
)
from pydantic import BaseModel, ValidationError
from prometheus_client import Counter, Gauge, Histogram
import redis.asyncio as redis

# Metrics Configuration
INVENTORY_UPDATES = Counter(
    'inventory_updates_total',
    'Total inventory updates',
    ['action', 'item_type']
)
INVENTORY_LEVELS = Gauge(
    'inventory_quantities',
    'Current inventory levels',
    ['item_id', 'location']
)
LOW_STOCK_ALERTS = Counter(
    'low_stock_alerts_total',
    'Low stock alert events'
)
INVENTORY_LOCK_TIME = Histogram(
    'inventory_lock_duration_seconds',
    'Inventory lock hold times'
)

class InventoryError(Exception):
    pass

class ItemNotFoundError(InventoryError):
    pass

class InsufficientStockError(InventoryError):
    pass

class InventoryLockError(InventoryError):
    pass

class StockAdjustmentType(Enum):
    INCREMENT = "increment"
    DECREMENT = "decrement"
    SET = "set"
    RESERVE = "reserve"
    RELEASE = "release"

@dataclass
class ItemBatch:
    batch_id: str
    item_id: str
    quantity: int
    expiration: Optional[datetime] = None
    location: str = "default"
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class InventoryTransaction:
    transaction_id: str
    item_id: str
    quantity: int
    adjustment_type: StockAdjustmentType
    reference_id: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    committed: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

class InventoryPlugin(ABC):
    @abstractmethod
    async def before_update(self, transaction: InventoryTransaction) -> None:
        pass
    
    @abstractmethod
    async def after_update(self, transaction: InventoryTransaction) -> None:
        pass

class InventoryStorage(ABC):
    @abstractmethod
    async def get_item_quantity(self, item_id: str, location: str) -> int:
        pass
    
    @abstractmethod
    async def update_item_quantity(
        self, 
        item_id: str,
        delta: int,
        location: str,
        transaction_id: Optional[str] = None
    ) -> int:
        pass
    
    @abstractmethod
    async def commit_transaction(self, transaction_id: str) -> bool:
        pass
    
    @abstractmethod
    async def rollback_transaction(self, transaction_id: str) -> bool:
        pass

class RedisInventoryStorage(InventoryStorage):
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.lock = asyncio.Lock()
    
    async def get_item_quantity(self, item_id: str, location: str) -> int:
        key = f"inventory:{item_id}:{location}"
        return int(await self.redis.get(key) or 0)
    
    async def update_item_quantity(
        self, 
        item_id: str,
        delta: int,
        location: str,
        transaction_id: Optional[str] = None
    ) -> int:
        key = f"inventory:{item_id}:{location}"
        async with self.lock:
            current = await self.get_item_quantity(item_id, location)
            new_value = current + delta
            if new_value < 0:
                raise InsufficientStockError(f"Insufficient stock for {item_id}")
            await self.redis.set(key, new_value)
            return new_value
    
    async def commit_transaction(self, transaction_id: str) -> bool:
        return True
    
    async def rollback_transaction(self, transaction_id: str) -> bool:
        return True

class InventoryManager:
    def __init__(
        self,
        storage: InventoryStorage,
        redis_client: Optional[redis.Redis] = None,
        low_stock_threshold: int = 10,
        lock_timeout: int = 30
    ):
        self.storage = storage
        self.redis = redis_client or redis.Redis()
        self.low_stock_threshold = low_stock_threshold
        self.lock_timeout = lock_timeout
        self.plugins: List[InventoryPlugin] = []
        self.active_locks: Dict[str, asyncio.Lock] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def register_plugin(self, plugin: InventoryPlugin):
        self.plugins.append(plugin)
    
    async def get_inventory(self, item_id: str, location: str = "default") -> int:
        quantity = await self.storage.get_item_quantity(item_id, location)
        INVENTORY_LEVELS.labels(item_id=item_id, location=location).set(quantity)
        return quantity
    
    async def adjust_inventory(
        self,
        item_id: str,
        delta: int,
        location: str = "default",
        transaction_id: Optional[str] = None
    ) -> int:
        lock_key = f"inventory_lock:{item_id}:{location}"
        async with self._get_lock(lock_key):
            transaction = InventoryTransaction(
                transaction_id=transaction_id or str(uuid.uuid4()),
                item_id=item_id,
                quantity=abs(delta),
                adjustment_type=(
                    StockAdjustmentType.INCREMENT if delta > 0 
                    else StockAdjustmentType.DECREMENT
                )
            )
            
            await self._execute_plugins('before_update', transaction)
            
            try:
                new_quantity = await self.storage.update_item_quantity(
                    item_id, delta, location, transaction.transaction_id
                )
                transaction.committed = True
                INVENTORY_UPDATES.labels(
                    action=transaction.adjustment_type.value,
                    item_type=item_id
                ).inc()
                
                await self._check_low_stock(item_id, new_quantity, location)
            except Exception as e:
                await self._execute_rollback(transaction)
                raise InventoryError(f"Inventory adjustment failed: {str(e)}") from e
            finally:
                await self._execute_plugins('after_update', transaction)
            
            return new_quantity
    
    async def batch_update(
        self,
        updates: List[Tuple[str, int, str]],
        transaction_id: Optional[str] = None
    ) -> Dict[str, int]:
        transaction_id = transaction_id or str(uuid.uuid4())
        results = {}
        
        try:
            for item_id, delta, location in updates:
                results[item_id] = await self.adjust_inventory(
                    item_id, delta, location, transaction_id
                )
            await self.storage.commit_transaction(transaction_id)
        except Exception as e:
            await self.storage.rollback_transaction(transaction_id)
            raise InventoryError(f"Batch update failed: {str(e)}") from e
        
        return results
    
    async def reserve_stock(
        self,
        item_id: str,
        quantity: int,
        location: str = "default",
        ttl: int = 3600
    ) -> str:
        reserve_id = str(uuid.uuid4())
        try:
            await self.adjust_inventory(item_id, -quantity, location, reserve_id)
            await self._set_reservation_ttl(reserve_id, ttl)
            return reserve_id
        except InsufficientStockError:
            raise
        except Exception as e:
            raise InventoryError(f"Reservation failed: {str(e)}") from e
    
    async def release_stock(self, reserve_id: str) -> None:
        try:
            await self.storage.rollback_transaction(reserve_id)
        except Exception as e:
            self.logger.error(f"Stock release failed: {str(e)}")
            raise InventoryError("Failed to release reserved stock") from e
    
    async def _get_lock(self, lock_key: str) -> AsyncGenerator[None, None]:
        if lock_key not in self.active_locks:
            self.active_locks[lock_key] = asyncio.Lock()
        
        lock = self.active_locks[lock_key]
        try:
            async with INVENTORY_LOCK_TIME.time():
                await asyncio.wait_for(lock.acquire(), timeout=self.lock_timeout)
                yield
        except asyncio.TimeoutError:
            raise InventoryLockError("Could not acquire inventory lock")
        finally:
            lock.release()
            if lock_key in self.active_locks and not lock.locked():
                del self.active_locks[lock_key]
    
    async def _check_low_stock(self, item_id: str, quantity: int, location: str):
        if quantity <= self.low_stock_threshold:
            LOW_STOCK_ALERTS.inc()
            self.logger.warning(
                f"Low stock alert: {item_id} in {location} "
                f"has {quantity} units remaining"
            )
    
    async def _execute_plugins(self, hook_name: str, transaction: InventoryTransaction):
        for plugin in self.plugins:
            try:
                if hook_name == 'before_update':
                    await plugin.before_update(transaction)
                elif hook_name == 'after_update':
                    await plugin.after_update(transaction)
            except Exception as e:
                self.logger.error(f"Plugin {plugin.__class__.__name__} failed: {str(e)}")
    
    async def _execute_rollback(self, transaction: InventoryTransaction):
        try:
            if transaction.committed:
                reverse_delta = (
                    -transaction.quantity 
                    if transaction.adjustment_type == StockAdjustmentType.INCREMENT
                    else transaction.quantity
                )
                await self.storage.update_item_quantity(
                    transaction.item_id,
                    reverse_delta,
                    "default",
                    transaction.transaction_id
                )
        except Exception as e:
            self.logger.error(f"Rollback failed: {str(e)}")
    
    async def _set_reservation_ttl(self, reserve_id: str, ttl: int):
        await self.redis.setex(f"reservation:{reserve_id}", ttl, "active")

class AuditPlugin(InventoryPlugin):
    async def before_update(self, transaction: InventoryTransaction) -> None:
        logging.info(f"Starting transaction: {transaction.transaction_id}")
    
    async def after_update(self, transaction: InventoryTransaction) -> None:
        status = "committed" if transaction.committed else "failed"
        logging.info(f"Transaction {transaction.transaction_id} {status}")

async def example_usage():
    redis_client = redis.Redis()
    storage = RedisInventoryStorage(redis_client)
    manager = InventoryManager(storage)
    manager.register_plugin(AuditPlugin())
    
    try:
        # Initial stock setup
        await manager.adjust_inventory("item_123", 100)
        
        # Batch update
        await manager.batch_update([
            ("item_456", 50, "warehouse"),
            ("item_789", -30, "storefront")
        ])
        
        # Stock reservation
        reserve_id = await manager.reserve_stock("item_123", 5)
        print(f"Reserved stock with ID: {reserve_id}")
        
        # Release reservation
        await manager.release_stock(reserve_id)
        
        current_stock = await manager.get_inventory("item_123")
        print(f"Current stock: {current_stock}")
        
    except InventoryError as e:
        print(f"Inventory error: {str(e)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(example_usage())
