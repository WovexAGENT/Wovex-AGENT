import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Any, Dict, Optional

from agent import BaseAgent, AgentConfig, TaskResult, ResourceLimits

@dataclass
class MockTask:
    task_id: str
    payload: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)

class TestBaseAgent(unittest.IsolatedAsyncioTestCase):
    
    def setUp(self):
        self.mock_task_handler = AsyncMock()
        self.mock_metrics_client = MagicMock()
        self.mock_alert_system = AsyncMock()
        
        self.agent_config = AgentConfig(
            agent_id="test_agent_1",
            poll_interval=0.1,
            max_retries=3,
            retry_delay=0.05,
            resource_limits=ResourceLimits(
                max_memory=1024,
                max_cpu=90,
                max_concurrent_tasks=2
            )
        )
        
        self.agent = BaseAgent(
            config=self.agent_config,
            task_handler=self.mock_task_handler,
            metrics_client=self.mock_metrics_client,
            alert_system=self.mock_alert_system
        )

    async def asyncTearDown(self):
        if self.agent.is_running:
            await self.agent.shutdown()

    async def test_agent_initialization(self):
        self.assertEqual(self.agent.agent_id, "test_agent_1")
        self.assertFalse(self.agent.is_running)
        self.assertEqual(self.agent.task_queue.qsize(), 0)
        self.assertEqual(self.agent.current_tasks, 0)

    async def test_task_processing_flow(self):
        test_task = MockTask("task_1", {"data": "sample"})
        
        # Mock task handler response
        self.mock_task_handler.execute_task.return_value = TaskResult(
            success=True,
            result={"processed": True},
            execution_time=0.1
        )
        
        # Start agent and submit task
        await self.agent.start()
        await self.agent.submit_task(test_task)
        
        # Allow processing time
        await asyncio.sleep(0.2)
        
        # Verify task handling
        self.mock_task_handler.execute_task.assert_awaited_once()
        self.mock_metrics_client.increment.assert_called_with("tasks_processed")
        self.assertEqual(self.agent.current_tasks, 0)

    async def test_concurrent_task_limiting(self):
        # Configure slow task processing
        async def slow_task_handler(*args):
            await asyncio.sleep(1)
            return TaskResult(success=True, result={})
            
        self.mock_task_handler.execute_task = slow_task_handler
        
        # Start agent and submit 3 tasks
        await self.agent.start()
        for i in range(3):
            await self.agent.submit_task(MockTask(f"task_{i}", {}))
            
        # Verify task queue state
        await asyncio.sleep(0.1)
        self.assertEqual(self.agent.current_tasks, 2)
        self.assertEqual(self.agent.task_queue.qsize(), 1)

    async def test_task_retry_mechanism(self):
        test_task = MockTask("retry_task", {"attempt": 0})
        
        # Configure failing task handler
        self.mock_task_handler.execute_task.side_effect = [
            Exception("Temporary failure"),
            Exception("Temporary failure"),
            TaskResult(success=True, result={})
        ]
        
        await self.agent.start()
        await self.agent.submit_task(test_task)
        
        # Allow retries to complete
        await asyncio.sleep(0.5)
        
        # Verify retry attempts
        self.assertEqual(self.mock_task_handler.execute_task.call_count, 3)
        self.mock_alert_system.notify_retry.assert_has_calls([
            call(test_task.task_id, 1),
            call(test_task.task_id, 2)
        ])

    async def test_critical_failure_handling(self):
        test_task = MockTask("critical_task", {})
        
        # Configure persistent failure
        self.mock_task_handler.execute_task.side_effect = Exception("Critical error")
        
        await self.agent.start()
        await self.agent.submit_task(test_task)
        
        # Allow processing attempts
        await asyncio.sleep(0.5)
        
        # Verify failure handling
        self.mock_alert_system.notify_failure.assert_awaited_with(
            test_task.task_id,
            "Critical error"
        )
        self.mock_metrics_client.increment.assert_called_with("tasks_failed")

    async def test_resource_monitoring(self):
        # Simulate resource exhaustion
        self.agent._check_resources = MagicMock(return_value=False)
        
        test_task = MockTask("resource_task", {})
        
        await self.agent.start()
        with self.assertRaises(RuntimeError):
            await self.agent.submit_task(test_task)
            
        self.mock_alert_system.notify_resource_limit.assert_awaited()

    async def test_graceful_shutdown(self):
        # Submit multiple tasks
        for i in range(5):
            await self.agent.submit_task(MockTask(f"shutdown_task_{i}", {}))
            
        # Start and immediately shutdown
        await self.agent.start()
        shutdown_time = await self.agent.shutdown()
        
        # Verify shutdown state
        self.assertGreaterEqual(shutdown_time, 0)
        self.assertFalse(self.agent.is_running)
        self.assertTrue(self.agent.task_queue.empty())

    async def test_heartbeat_mechanism(self):
        with patch('agent.datetime') as mock_datetime:
            fixed_time = datetime(2023, 1, 1)
            mock_datetime.now.return_value = fixed_time
            
            await self.agent.start()
            await asyncio.sleep(0.15)  # Allow one heartbeat
            
            self.mock_metrics_client.gauge.assert_called_with(
                "agent_heartbeat",
                fixed_time.timestamp()
            )

    async def test_config_override(self):
        custom_config = AgentConfig(
            agent_id="custom_agent",
            poll_interval=0.05,
            max_retries=5
        )
        
        agent = BaseAgent(
            config=custom_config,
            task_handler=self.mock_task_handler
        )
        
        self.assertEqual(agent.config.max_retries, 5)
        self.assertEqual(agent.config.poll_interval, 0.05)

    async def test_task_prioritization(self):
        high_pri_task = MockTask("high_pri", {}, {"priority": 5})
        low_pri_task = MockTask("low_pri", {}, {"priority": 1})
        
        await self.agent.start()
        await self.agent.submit_task(low_pri_task)
        await self.agent.submit_task(high_pri_task)
        
        # Verify task processing order
        processing_order = []
        self.mock_task_handler.execute_task.side_effect = lambda task: processing_order.append(task.task_id)
        
        await asyncio.sleep(0.3)
        self.assertEqual(processing_order, ["high_pri", "low_pri"])

if __name__ == "__main__":
    unittest.main()
