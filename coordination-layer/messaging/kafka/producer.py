import asyncio
import logging
import signal
import time
import uuid
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, AsyncGenerator, Callable
from functools import partial
from pydantic import BaseModel, BaseSettings, AnyUrl, Field
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from confluent_kafka import Producer as KafkaProducer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# ======================
# Configuration Models
# ======================

class ProducerConfig(BaseSettings):
    bootstrap_servers: List[AnyUrl] = Field(..., env="KAFKA_BOOTSTRAP_SERVERS")
    ssl_cafile: Optional[str] = Field(None, env="KAFKA_SSL_CAFILE")
    ssl_certfile: Optional[str] = Field(None, env="KAFKA_SSL_CERTFILE")
    ssl_keyfile: Optional[str] = Field(None, env="KAFKA_SSL_KEYFILE")
    sasl_mechanism: Optional[str] = Field(None, env="KAFKA_SASL_MECHANISM")
    sasl_username: Optional[str] = Field(None, env="KAFKA_SASL_USERNAME")
    sasl_password: Optional[str] = Field(None, env="KAFKA_SASL_PASSWORD")
    message_timeout_ms: int = 30000
    max_in_flight: int = 100000
    retry_backoff_ms: int = 100
    compression_type: str = 'snappy'
    metrics_port: int = 9092
    enable_idempotence: bool = True
    max_batch_size: int = 16384
    linger_ms: int = 5
    request_timeout_ms: int = 30000

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# ======================
# Data Models
# ======================

class MessageMetadata(BaseModel):
    message_id: uuid.UUID
    timestamp: datetime
    attempt: int = 0
    headers: Dict[str, str] = {}

class ProducerMessage(BaseModel):
    topic: str
    key: Optional[bytes] = None
    value: bytes
    metadata: MessageMetadata
    partition: Optional[int] = None

# ======================
# Metrics
# ======================

MESSAGES_SENT = Counter(
    'producer_messages_total',
    'Total messages sent',
    ['topic', 'status']
)

MESSAGE_LATENCY = Histogram(
    'producer_message_latency_seconds',
    'Message delivery latency histogram',
    ['topic']
)

PRODUCER_BUFFER_GAUGE = Gauge(
    'producer_buffer_size',
    'Current producer buffer size'
)

DELIVERY_ERRORS = Counter(
    'producer_delivery_errors_total',
    'Total message delivery errors'
)

# ======================
# Producer Implementation
# ======================

class AsyncMessageProducer:
    def __init__(self, config: ProducerConfig):
        self.config = config
        self._producer = None
        self._admin_client = None
        self._running = False
        self._delivery_callback = None
        self._pending_messages = {}

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def _configure_producer(self) -> KafkaProducer:
        conf = {
            'bootstrap.servers': ','.join(str(s) for s in self.config.bootstrap_servers),
            'message.timeout.ms': self.config.message_timeout_ms,
            'max.in.flight.requests.per.connection': self.config.max_in_flight,
            'retry.backoff.ms': self.config.retry_backoff_ms,
            'compression.type': self.config.compression_type,
            'enable.idempotence': self.config.enable_idempotence,
            'linger.ms': self.config.linger_ms,
            'batch.size': self.config.max_batch_size,
            'request.timeout.ms': self.config.request_timeout_ms,
            'queue.buffering.max.messages': 1000000,
            'queue.buffering.max.kbytes': 1048576,
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

        return KafkaProducer(conf)

    async def start(self):
        """Initialize the producer and admin client"""
        self._producer = self._configure_producer()
        self._admin_client = AdminClient({'bootstrap.servers': self.config.bootstrap_servers})
        self._running = True
        start_http_server(self.config.metrics_port)
        logging.info("Producer initialized and metrics server started")

    async def close(self):
        """Cleanup producer resources"""
        self._running = False
        if self._producer:
            remaining = self._producer.flush(timeout=10)
            if remaining > 0:
                logging.warning(f"Failed to flush {remaining} messages")
            self._producer = None
        logging.info("Producer shutdown completed")

    def _delivery_report(self, err: Optional[Exception], msg: ProducerMessage):
        """Handle message delivery callbacks"""
        start_time = self._pending_messages.pop(id(msg), None)
        latency = time.time() - start_time if start_time else 0

        if err is not None:
            DELIVERY_ERRORS.inc()
            logging.error(f"Message delivery failed: {str(err)}")
            MESSAGES_SENT.labels(topic=msg.topic, status='error').inc()
        else:
            MESSAGE_LATENCY.labels(topic=msg.topic).observe(latency)
            MESSAGES_SENT.labels(topic=msg.topic, status='success').inc()
            logging.debug(f"Message delivered to {msg.topic} [{msg.partition or 'any'}]")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=10),
        retry=retry_if_exception_type(KafkaException)
    )
    async def produce(self, message: ProducerMessage):
        """Asynchronously produce a message"""
        if not self._running:
            raise RuntimeError("Producer is not running")

        start_time = time.time()
        self._pending_messages[id(message)] = start_time

        try:
            self._producer.produce(
                topic=message.topic,
                key=message.key,
                value=message.value,
                headers=message.metadata.headers,
                partition=message.partition,
                callback=partial(self._delivery_report, msg=message)
            )
            
            PRODUCER_BUFFER_GAUGE.set(len(self._producer))
            logging.debug(f"Message {message.metadata.message_id} queued")

        except BufferError as e:
            logging.error("Producer queue full")
            DELIVERY_ERRORS.inc()
            raise
        except KafkaException as e:
            logging.error(f"Production error: {str(e)}")
            DELIVERY_ERRORS.inc()
            raise

    async def create_topic(self, topic_name: str, partitions: int = 3, replication: int = 2):
        """Create a new topic if not exists"""
        new_topic = NewTopic(
            topic_name,
            num_partitions=partitions,
            replication_factor=replication,
            config={
                'retention.ms': '604800000',  # 7 days
                'cleanup.policy': 'compact'
            }
        )

        fs = self._admin_client.create_topics([new_topic])
        for topic, future in fs.items():
            try:
                future.result()
                logging.info(f"Topic {topic} created")
            except KafkaException as e:
                if e.args[0].code() == KafkaException.TOPIC_ALREADY_EXISTS:
                    logging.debug(f"Topic {topic} already exists")
                else:
                    raise

    async def monitor_buffer(self):
        """Background task for monitoring producer state"""
        while self._running:
            buffer_size = len(self._producer)
            PRODUCER_BUFFER_GAUGE.set(buffer_size)
            await asyncio.sleep(5)

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
# Main Execution
# ======================

async def example_producer():
    config = ProducerConfig()
    async with AsyncMessageProducer(config) as producer:
        # Example message production
        message = ProducerMessage(
            topic="transactions",
            value=json.dumps({"amount": 100.50, "currency": "USD"}).encode('utf-8'),
            metadata=MessageMetadata(
                message_id=uuid.uuid4(),
                timestamp=datetime.utcnow(),
                headers={"source": "payment-service"}
            )
        )
        
        await producer.create_topic("transactions")
        await producer.produce(message)
        await asyncio.sleep(1)  # Allow time for delivery

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    asyncio.run(example_producer())
