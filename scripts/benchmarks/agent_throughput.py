#!/usr/bin/env python3
import asyncio
import time
import argparse
import logging
import statistics
import random
import json
from dataclasses import dataclass
from typing import List, Dict, Optional, Awaitable
from contextlib import asynccontextmanager

# Metrics Collection
@dataclass
class ThroughputMetrics:
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    latency_samples: List[float] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    last_reset: float = field(default_factory=time.time)

    @property
    def duration(self) -> float:
        return time.time() - self.start_time

    @property
    def requests_per_second(self) -> float:
        return self.total_requests / self.duration if self.duration > 0 else 0

    @property
    def avg_latency(self) -> float:
        return statistics.mean(self.latency_samples) if self.latency_samples else 0

    @property
    def p95_latency(self) -> float:
        return statistics.quantiles(self.latency_samples, n=100)[94] if self.latency_samples else 0

    @property
    def max_latency(self) -> float:
        return max(self.latency_samples) if self.latency_samples else 0

    @property
    def min_latency(self) -> float:
        return min(self.latency_samples) if self.latency_samples else 0

    def reset(self) -> None:
        self.__init__()

class AgentLoadTester:
    def __init__(self, config: argparse.Namespace):
        self.config = config
        self.metrics = ThroughputMetrics()
        self.logger = self._configure_logger()
        self._request_semaphore = asyncio.Semaphore(config.concurrency)

    def _configure_logger(self) -> logging.Logger:
        logger = logging.getLogger("AgentThroughput")
        logger.setLevel(logging.DEBUG if self.config.verbose else logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger

    async def _execute_single_request(self, request_id: int) -> None:
        async with self._request_semaphore:
            start_time = time.monotonic()
            try:
                # Implement actual agent request logic here
                await asyncio.sleep(random.uniform(0.1, 0.5))  # Simulate request processing
                
                if random.random() < self.config.error_rate:
                    raise Exception("Simulated error")
                    
                latency = time.monotonic() - start_time
                self.metrics.latency_samples.append(latency)
                self.metrics.successful_requests += 1
            except Exception as e:
                self.logger.error(f"Request {request_id} failed: {str(e)}")
                self.metrics.failed_requests += 1
            finally:
                self.metrics.total_requests += 1

    async def _generate_load(self) -> None:
        tasks: List[Awaitable] = []
        for i in range(self.config.requests):
            task = asyncio.create_task(self._execute_single_request(i))
            tasks.append(task)
            
            if len(tasks) >= self.config.batch_size:
                await asyncio.gather(*tasks)
                tasks.clear()
                
        if tasks:
            await asyncio.gather(*tasks)

    async def _monitor_progress(self) -> None:
        while True:
            await asyncio.sleep(self.config.monitor_interval)
            self._report_metrics()

    def _report_metrics(self) -> None:
        report = {
            "timestamp": time.time(),
            "requests_issued": self.metrics.total_requests,
            "rps": round(self.metrics.requests_per_second, 2),
            "avg_latency": round(self.metrics.avg_latency, 4),
            "p95_latency": round(self.metrics.p95_latency, 4),
            "success_rate": round(
                self.metrics.successful_requests / self.metrics.total_requests, 2
            )
            if self.metrics.total_requests > 0
            else 0,
        }
        
        if self.config.output_format == "json":
            print(json.dumps(report))
        else:
            print(
                f"Requests: {report['requests_issued']} | "
                f"RPS: {report['rps']} | "
                f"Latency: {report['avg_latency']}s (avg) / {report['p95_latency']}s (p95) | "
                f"Success: {report['success_rate']*100}%"
            )

    async def run(self) -> None:
        self.logger.info("Starting throughput test...")
        async with self._resource_manager():
            monitor_task = asyncio.create_task(self._monitor_progress())
            await self._generate_load()
            monitor_task.cancel()
            self._report_metrics()

    @asynccontextmanager
    async def _resource_manager(self):
        try:
            yield
        except asyncio.CancelledError:
            self.logger.warning("Test interrupted by user")
        except Exception as e:
            self.logger.error(f"Critical error occurred: {str(e)}")
            raise
        finally:
            self.logger.info("Test completed. Cleaning up resources...")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Agent Throughput Tester")
    
    parser.add_argument(
        "-r", "--requests", 
        type=int, 
        default=1000,
        help="Total number of requests to send"
    )
    
    parser.add_argument(
        "-c", "--concurrency", 
        type=int, 
        default=100,
        help="Maximum concurrent requests"
    )
    
    parser.add_argument(
        "-b", "--batch-size", 
        type=int, 
        default=50,
        help="Batch size for request grouping"
    )
    
    parser.add_argument(
        "-e", "--error-rate", 
        type=float, 
        default=0.05,
        help="Simulated error rate (0.0-1.0)"
    )
    
    parser.add_argument(
        "-i", "--monitor-interval", 
        type=float, 
        default=1.0,
        help="Metrics reporting interval in seconds"
    )
    
    parser.add_argument(
        "-f", "--output-format", 
        choices=["text", "json"], 
        default="text",
        help="Output format for metrics reporting"
    )
    
    parser.add_argument(
        "-v", "--verbose", 
        action="store_true",
        help="Enable verbose logging"
    )
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    tester = AgentLoadTester(args)
    
    try:
        asyncio.run(tester.run())
    except KeyboardInterrupt:
        print("\nTest aborted by user")
