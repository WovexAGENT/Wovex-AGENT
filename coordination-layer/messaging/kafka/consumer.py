import asyncio
import logging
import signal
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Callable, Awaitable
from functools import partial
from pydantic import BaseModel, BaseSettings, AnyUrl, Field, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from confluent_kafka import Consumer as KafkaConsumer, KafkaException, TopicPartition

# ======================
# Configuration Models
# ======================

class ConsumerConfig(BaseSettings):
    bootstrap_servers: List[AnyUrl] = Field(..., env="KAFKA_BOOTSTRAP_SERVERS")
    group_id: str = Field(..., env="CONSUMER_GROUP_ID")
    ssl_cafile: Optional[str] = Field(None, env="KAFKA_SSL_CAFILE")
    ssl_certfile: Optional[str] = Field(None, env="KAFKA_SSL_CERTFILE")
    ssl_keyfile: Optional[str] = Field(None, env="KAFKA_SSL_KEYFILE")
    sasl_mechanism: Optional[str] = Field(None, env="KAFKA_SASL_MECHANISM")
    sasl_username: Optional[str] = Field(None, env="KAFKA_SASL_USERNAME")
    sasl_password: Optional[str] = Field(None, env="KAFKA_SASL_PASSWORD")
    auto_offset_reset: str = 'earliest'
    enable_auto_commit: bool = False
    max_poll_interval_ms: int = 300000
    max_poll_records: int = 500
    session_timeout_ms: int = 10000
    heartbeat_interval_ms: int = 3000
    isolation_level: str = 'read_committed'
    metrics_port: int = 9093
    dead_letter_topic: str = 'dead_letter_queue'
    max_retries: int = 3
    retry_backoff_ms: int = 1000

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# ======================
# Data Models
# ======================

class ConsumerMessage(BaseModel):
    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: bytes
    timestamp: datetime
    headers: Dict[str, bytes] = {}
    delivery_attempt: int = 0

class ProcessResult(BaseModel):
    success: bool
    retryable: bool = False
    error_message: Optional[str] = None

# ======================
# Metrics
# ======================

MESSAGES_CONSUMED = Counter(
    'consumer_messages_total',
    'Total messages consumed',
    ['topic', 'status']
)

PROCESSING_LATENCY = Histogram(
    'consumer_processing_latency_seconds',
    'Message processing latency histogram',
    ['topic']
)

CONSUMER_LAG = Gauge(
    'consumer_lag_seconds',
    'Consumer lag in seconds',
    ['topic', 'partition']
)

CONSUMER_OFFSET = Gauge(
    'consumer_current_offset',
    'Current consumer offset',
    ['topic', 'partition']
)

DEAD_LETTERS = Counter(
    'dead_letter_messages_total',
    'Messages sent to dead letter queue'
)

# ======================
# Consumer Implementation
# ======================

class AsyncMessageConsumer:
    def __init__(self, config: ConsumerConfig):
        self.config = config
        self._consumer = None
        self._running = False
        self._processing_tasks = set()
        self._producer = None  # For dead letter queue

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def _configure_consumer(self) -> KafkaConsumer:
        conf = {
            'bootstrap.servers': ','.join(str(s) for s in self.config.bootstrap_servers),
            'group.id': self.config.group_id,
            'auto.offset.reset': self.config.auto_offset_reset,
            'enable.auto.commit': self.config.enable_auto_commit,
            'max.poll.interval.ms': self.config.max_poll_interval_ms,
            'max.poll.records': self.config.max_poll_records,
            'session.timeout.ms': self.config.session_timeout_ms,
            'heartbeat.interval.ms': self.config.heartbeat_interval_ms,
            'isolation.level': self.config.isolation_level,
        }

        if self.config.ssl_cafile:
            conf.update({
                'security.protocol': 'SSL',
                'ssl.ca.location': self.config.ssl_cafile,
                'ssl.certificate.location': self.config.ssl_certfile,
                'ssl.key.location': self.config.ssl_keyfile,
            })

        if self.config.sasl_mechanism:
            conf.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': self.config.sasl_mechanism,
                'sasl.username': self.config.sasl_username,
                'sasl.password': self.config.sasl_password,
            })

        return KafkaConsumer(conf)

    async def start(self):
        """Initialize consumer and metrics server"""
        self._consumer = self._configure_consumer()
        self._running = True
        start_http_server(self.config.metrics_port)
        logging.info("Consumer initialized and metrics server started")

    async def close(self):
        """Cleanup consumer resources"""
        self._running = False
        if self._consumer:
            self._consumer.close()
        logging.info("Consumer shutdown completed")

    async def subscribe(self, topics: List[str]):
        """Subscribe to list of topics"""
        self._consumer.subscribe(topics, on_assign=self._on_assign, on_revoke=self._on_revoke)
        logging.info(f"Subscribed to topics: {', '.join(topics)}")

    def _on_assign(self, consumer: KafkaConsumer, partitions: list):
        """Callback when partitions are assigned"""
        logging.info(f"Assigned partitions: {partitions}")
        for p in partitions:
            CONSUMER_OFFSET.labels(topic=p.topic, partition=p.partition).set(p.offset)

    def _on_revoke(self, consumer: KafkaConsumer, partitions: list):
        """Callback when partitions are revoked"""
        logging.info(f"Revoked partitions: {partitions}")
        self._consumer.unassign()

    async def _get_producer(self):
        """Lazy initialization of dead letter producer"""
        if not self._producer:
            from .producer import AsyncMessageProducer, ProducerConfig
            producer_config = ProducerConfig(
                bootstrap_servers=self.config.bootstrap_servers,
                ssl_cafile=self.config.ssl_cafile,
                ssl_certfile=self.config.ssl_certfile,
                ssl_keyfile=self.config.ssl_keyfile,
                sasl_mechanism=self.config.sasl_mechanism,
                sasl_username=self.config.sasl_username,
                sasl_password=self.config.sasl_password,
            )
            self._producer = AsyncMessageProducer(producer_config)
            await self._producer.start()
        return self._producer

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=10),
        retry=retry_if_exception_type(KafkaException)
    )
    async def _send_to_dlq(self, message: ConsumerMessage, error: str):
        """Send failed message to dead letter queue"""
        producer = await self._get_producer()
        dlq_message = {
            'original_topic': message.topic,
            'original_partition': message.partition,
            'original_offset': message.offset,
            'payload': message.value.decode('utf-8'),
            'error': error,
            'timestamp': datetime.utcnow().isoformat()
        }
        await producer.produce(
            topic=self.config.dead_letter_topic,
            value=json.dumps(dlq_message).encode('utf-8')
        )
        DEAD_LETTERS.inc()

    async def _process_message(self, message: ConsumerMessage, handler: Callable[[ConsumerMessage], Awaitable[ProcessResult]]):
        """Process individual message with retries"""
        try:
            start_time = time.time()
            result = await handler(message)
            latency = time.time() - start_time
            PROCESSING_LATENCY.labels(topic=message.topic).observe(latency)

            if result.success:
                MESSAGES_CONSUMED.labels(topic=message.topic, status='success').inc()
            else:
                if result.retryable and message.delivery_attempt < self.config.max_retries:
                    MESSAGES_CONSUMED.labels(topic=message.topic, status='retry').inc()
                    raise KafkaException(f"Retryable error: {result.error_message}")
                else:
                    await self._send_to_dlq(message, result.error_message)
                    MESSAGES_CONSUMED.labels(topic=message.topic, status='failed').inc()

        except (ValidationError, json.JSONDecodeError) as e:
            await self._send_to_dlq(message, f"Invalid message format: {str(e)}")
            MESSAGES_CONSUMED.labels(topic=message.topic, status='invalid').inc()
        except Exception as e:
            logging.error(f"Unexpected error processing message: {str(e)}")
            await self._send_to_dlq(message, f"Processing error: {str(e)}")
            MESSAGES_CONSUMED.labels(topic=message.topic, status='error').inc()

    async def consume(self, handler: Callable[[ConsumerMessage], Awaitable[ProcessResult]]):
        """Main consumption loop"""
        while self._running:
            try:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                message = ConsumerMessage(
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    key=msg.key(),
                    value=msg.value(),
                    timestamp=datetime.fromtimestamp(msg.timestamp()[1] / 1000),
                    headers=msg.headers(),
                )

                task = asyncio.create_task(self._process_message(message, handler))
                self._processing_tasks.add(task)
                task.add_done_callback(self._processing_tasks.discard)

                # Update metrics
                CONSUMER_LAG.labels(topic=msg.topic(), partition=msg.partition()).set(
                    (datetime.now() - message.timestamp).total_seconds()
                )
                CONSUMER_OFFSET.labels(topic=msg.topic(), partition=msg.partition()).set(msg.offset())

            except KafkaException as e:
                logging.error(f"Consumption error: {str(e)}")
                await asyncio.sleep(1)

    async def commit_offsets(self):
        """Manually commit offsets"""
        self._consumer.commit()

# ======================
# Signal Handling
# ======================

def handle_shutdown(sig, frame):
    logging.info("Initiating graceful shutdown...")
    loop = asyncio.get_event_loop()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    
    for task in tasks:
        task.cancel()
    
    loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
    loop.stop()

# ======================
# Example Usage
# ======================

async def example_handler(message: ConsumerMessage) -> ProcessResult:
    try:
        payload = json.loads(message.value.decode('utf-8'))
        # Implement business logic here
        await asyncio.sleep(0.1)  # Simulate processing
        return ProcessResult(success=True)
    except Exception as e:
        return ProcessResult(
            success=False,
            retryable=True,
            error_message=str(e)
        )

async def main():
    config = ConsumerConfig()
    async with AsyncMessageConsumer(config) as consumer:
        await consumer.subscribe(['transactions'])
        await consumer.consume(example_handler)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    asyncio.run(main())
