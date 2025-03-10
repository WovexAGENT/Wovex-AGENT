import unittest
import numpy as np
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import datetime
from typing import Dict, Any, List
from collections import defaultdict

# Assume the following imports from your codebase
# from federated_learning import FederatedCoordinator, ClientNode, ModelAggregator
# from differential_privacy import GaussianMechanism

class TestFederatedLearningSystem(unittest.TestCase):
    
    def setUp(self):
        self.mock_logger = MagicMock()
        self.mock_metrics = MagicMock()
        
        # Mock base components
        self.mock_aggregator = MagicMock()
        self.mock_aggregator.aggregate.return_value = MagicMock()
        
        # Configure test clients
        self.clients = [
            self._create_mock_client(f"client_{i}", i%2==0)
            for i in range(5)
        ]
        
        # Initialize coordinator
        with patch('federated_learning.ModelAggregator') as mock_agg:
            mock_agg.return_value = self.mock_aggregator
            self.coordinator = FederatedCoordinator(
                clients=self.clients,
                logger=self.mock_logger,
                metrics=self.mock_metrics
            )

    def _create_mock_client(self, client_id: str, is_secure: bool):
        client = MagicMock()
        client.id = client_id
        client.is_secure = is_secure
        client.get_model_update = AsyncMock()
        client.send_global_model = AsyncMock()
        return client

    def test_normal_training_round(self):
        # Setup mock model updates
        mock_updates = [
            {'weights': np.random.rand(10), 'samples': 100},
            {'weights': np.random.rand(10), 'samples': 200}
        ]
        for client in self.clients:
            client.get_model_update.return_value = mock_updates.pop() if mock_updates else None

        # Execute training round
        async def run_test():
            await self.coordinator.run_training_round()
            
        asyncio.run(run_test())

        # Verify aggregation
        self.mock_aggregator.aggregate.assert_called_once()
        self.assertEqual(self.mock_aggregator.aggregate.call_args[0][0].shape, (10,))
        
        # Verify metrics collection
        self.mock_metrics.increment.assert_any_call(
            'fl.rounds.completed',
            tags={'status': 'success'}
        )
        self.mock_metrics.gauge.assert_any_call(
            'fl.clients.participating',
            value=5
        )

    def test_differential_privacy_application(self):
        # Configure DP mechanism
        self.coordinator.privacy_mechanism = MagicMock()
        self.coordinator.privacy_mechanism.apply.return_value = np.zeros(10)
        
        # Execute training round
        async def run_test():
            await self.coordinator.run_training_round()
            
        asyncio.run(run_test())

        # Verify DP application
        self.coordinator.privacy_mechanism.apply.assert_called_once()
        noise_std = np.std(self.coordinator.privacy_mechanism.apply.call_args[0][0])
        self.assertLess(noise_std, 0.5)  # Adjust based on DP params

    def test_client_dropout_handling(self):
        # Simulate client dropout
        for client in self.clients[:2]:
            client.get_model_update.side_effect = TimeoutError("Connection lost")

        async def run_test():
            await self.coordinator.run_training_round()
            
        asyncio.run(run_test())

        # Verify error handling
        self.mock_logger.error.assert_any_call(
            "Client connection failed",
            extra={'client_id': 'client_0'}
        )
        self.mock_metrics.increment.assert_any_call(
            'fl.clients.errors',
            tags={'error_type': 'timeout'}
        )
        self.assertEqual(self.mock_aggregator.aggregate.call_args[0][1], 3)

    def test_model_poisoning_detection(self):
        # Inject malicious update
        malicious_update = {'weights': np.ones(10)*100, 'samples': 100}
        self.clients[0].get_model_update.return_value = malicious_update
        
        with patch('federated_learning.anomaly_detector') as mock_detector:
            mock_detector.detect.return_value = True
            
            async def run_test():
                await self.coordinator.run_training_round()
                
            asyncio.run(run_test())

            # Verify detection
            mock_detector.detect.assert_called_once()
            self.mock_metrics.increment.assert_any_call(
                'fl.security.anomalies',
                tags={'type': 'model_poisoning'}
            )

    def test_secure_communication(self):
        # Verify TLS configuration for secure clients
        for client in self.clients:
            if client.is_secure:
                client.secure_channel.assert_called_with(
                    certificate_path='/path/to/cert.pem',
                    private_key_path='/path/to/key.pem'
                )
                
        # Verify insecure client handling
        insecure_clients = [c for c in self.clients if not c.is_secure]
        self.assertGreater(len(insecure_clients), 0)
        self.mock_logger.warning.assert_called_with(
            "Insecure client connection detected",
            extra={'client_id': insecure_clients[0].id}
        )

    def test_model_version_control(self):
        initial_version = self.coordinator.global_model.version
        
        async def run_round():
            await self.coordinator.run_training_round()
            
        asyncio.run(run_round())
        
        # Verify version increment
        self.assertEqual(
            self.coordinator.global_model.version,
            initial_version + 1
        )
        self.mock_metrics.gauge.assert_any_call(
            'fl.model.version',
            value=initial_version + 1
        )

    def test_resource_utilization(self):
        with patch('psutil.cpu_percent') as mock_cpu, \
             patch('psutil.virtual_memory') as mock_mem:
            mock_cpu.return_value = 75.5
            mock_mem.return_value.percent = 65.0
            
            async def run_test():
                await self.coordinator.monitor_resources()
                
            asyncio.run(run_test())

            self.mock_metrics.gauge.assert_any_call(
                'system.cpu.usage',
                value=75.5
            )
            self.mock_metrics.gauge.assert_any_call(
                'system.memory.usage',
                value=65.0
            )

    def test_federated_averaging_correctness(self):
        # Test weighted average calculation
        updates = [
            (np.array([1.0, 2.0]), 100),
            (np.array([3.0, 4.0]), 200)
        ]
        total_samples = sum(s for _, s in updates)
        
        # Calculate expected average
        expected = sum(w * s for w, s in updates) / total_samples
        self.coordinator.aggregator.aggregate.return_value = expected
        
        async def run_test():
            result = await self.coordinator.aggregate_updates(updates)
            np.testing.assert_array_equal(result, expected)
            
        asyncio.run(run_test())

    def test_round_timeout_handling(self):
        # Simulate slow client
        async def slow_update():
            await asyncio.sleep(10)
            return {'weights': np.zeros(10), 'samples': 50}
        self.clients[0].get_model_update = slow_update
        
        with self.assertRaises(asyncio.TimeoutError):
            async def run_test():
                await self.coordinator.run_training_round(timeout=2)
                
            asyncio.run(run_test())

        self.mock_metrics.increment.assert_any_call(
            'fl.rounds.failed',
            tags={'reason': 'timeout'}
        )

    def test_client_selection_strategy(self):
        # Test different selection strategies
        strategies = [
            ('random', lambda x: x[:3]),
            ('stratified', lambda x: sorted(x, key=lambda c: c.id)[::2])
        ]
        
        for strategy_name, selection_fn in strategies:
            with self.subTest(strategy=strategy_name):
                self.coordinator.client_selection = selection_fn
                selected = self.coordinator.select_clients()
                self.assertEqual(len(selected), 3 if strategy_name == 'random' else 3)
                self.mock_metrics.increment.assert_any_call(
                    'fl.clients.selected',
                    tags={'strategy': strategy_name}
                )

if __name__ == '__main__':
    unittest.main()
import unittest
import numpy as np
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import datetime
from typing import Dict, Any, List
from collections import defaultdict

# Assume the following imports from your codebase
# from federated_learning import FederatedCoordinator, ClientNode, ModelAggregator
# from differential_privacy import GaussianMechanism

class TestFederatedLearningSystem(unittest.TestCase):
    
    def setUp(self):
        self.mock_logger = MagicMock()
        self.mock_metrics = MagicMock()
        
        # Mock base components
        self.mock_aggregator = MagicMock()
        self.mock_aggregator.aggregate.return_value = MagicMock()
        
        # Configure test clients
        self.clients = [
            self._create_mock_client(f"client_{i}", i%2==0)
            for i in range(5)
        ]
        
        # Initialize coordinator
        with patch('federated_learning.ModelAggregator') as mock_agg:
            mock_agg.return_value = self.mock_aggregator
            self.coordinator = FederatedCoordinator(
                clients=self.clients,
                logger=self.mock_logger,
                metrics=self.mock_metrics
            )

    def _create_mock_client(self, client_id: str, is_secure: bool):
        client = MagicMock()
        client.id = client_id
        client.is_secure = is_secure
        client.get_model_update = AsyncMock()
        client.send_global_model = AsyncMock()
        return client

    def test_normal_training_round(self):
        # Setup mock model updates
        mock_updates = [
            {'weights': np.random.rand(10), 'samples': 100},
            {'weights': np.random.rand(10), 'samples': 200}
        ]
        for client in self.clients:
            client.get_model_update.return_value = mock_updates.pop() if mock_updates else None

        # Execute training round
        async def run_test():
            await self.coordinator.run_training_round()
            
        asyncio.run(run_test())

        # Verify aggregation
        self.mock_aggregator.aggregate.assert_called_once()
        self.assertEqual(self.mock_aggregator.aggregate.call_args[0][0].shape, (10,))
        
        # Verify metrics collection
        self.mock_metrics.increment.assert_any_call(
            'fl.rounds.completed',
            tags={'status': 'success'}
        )
        self.mock_metrics.gauge.assert_any_call(
            'fl.clients.participating',
            value=5
        )

    def test_differential_privacy_application(self):
        # Configure DP mechanism
        self.coordinator.privacy_mechanism = MagicMock()
        self.coordinator.privacy_mechanism.apply.return_value = np.zeros(10)
        
        # Execute training round
        async def run_test():
            await self.coordinator.run_training_round()
            
        asyncio.run(run_test())

        # Verify DP application
        self.coordinator.privacy_mechanism.apply.assert_called_once()
        noise_std = np.std(self.coordinator.privacy_mechanism.apply.call_args[0][0])
        self.assertLess(noise_std, 0.5)  # Adjust based on DP params

    def test_client_dropout_handling(self):
        # Simulate client dropout
        for client in self.clients[:2]:
            client.get_model_update.side_effect = TimeoutError("Connection lost")

        async def run_test():
            await self.coordinator.run_training_round()
            
        asyncio.run(run_test())

        # Verify error handling
        self.mock_logger.error.assert_any_call(
            "Client connection failed",
            extra={'client_id': 'client_0'}
        )
        self.mock_metrics.increment.assert_any_call(
            'fl.clients.errors',
            tags={'error_type': 'timeout'}
        )
        self.assertEqual(self.mock_aggregator.aggregate.call_args[0][1], 3)

    def test_model_poisoning_detection(self):
        # Inject malicious update
        malicious_update = {'weights': np.ones(10)*100, 'samples': 100}
        self.clients[0].get_model_update.return_value = malicious_update
        
        with patch('federated_learning.anomaly_detector') as mock_detector:
            mock_detector.detect.return_value = True
            
            async def run_test():
                await self.coordinator.run_training_round()
                
            asyncio.run(run_test())

            # Verify detection
            mock_detector.detect.assert_called_once()
            self.mock_metrics.increment.assert_any_call(
                'fl.security.anomalies',
                tags={'type': 'model_poisoning'}
            )

    def test_secure_communication(self):
        # Verify TLS configuration for secure clients
        for client in self.clients:
            if client.is_secure:
                client.secure_channel.assert_called_with(
                    certificate_path='/path/to/cert.pem',
                    private_key_path='/path/to/key.pem'
                )
                
        # Verify insecure client handling
        insecure_clients = [c for c in self.clients if not c.is_secure]
        self.assertGreater(len(insecure_clients), 0)
        self.mock_logger.warning.assert_called_with(
            "Insecure client connection detected",
            extra={'client_id': insecure_clients[0].id}
        )

    def test_model_version_control(self):
        initial_version = self.coordinator.global_model.version
        
        async def run_round():
            await self.coordinator.run_training_round()
            
        asyncio.run(run_round())
        
        # Verify version increment
        self.assertEqual(
            self.coordinator.global_model.version,
            initial_version + 1
        )
        self.mock_metrics.gauge.assert_any_call(
            'fl.model.version',
            value=initial_version + 1
        )

    def test_resource_utilization(self):
        with patch('psutil.cpu_percent') as mock_cpu, \
             patch('psutil.virtual_memory') as mock_mem:
            mock_cpu.return_value = 75.5
            mock_mem.return_value.percent = 65.0
            
            async def run_test():
                await self.coordinator.monitor_resources()
                
            asyncio.run(run_test())

            self.mock_metrics.gauge.assert_any_call(
                'system.cpu.usage',
                value=75.5
            )
            self.mock_metrics.gauge.assert_any_call(
                'system.memory.usage',
                value=65.0
            )

    def test_federated_averaging_correctness(self):
        # Test weighted average calculation
        updates = [
            (np.array([1.0, 2.0]), 100),
            (np.array([3.0, 4.0]), 200)
        ]
        total_samples = sum(s for _, s in updates)
        
        # Calculate expected average
        expected = sum(w * s for w, s in updates) / total_samples
        self.coordinator.aggregator.aggregate.return_value = expected
        
        async def run_test():
            result = await self.coordinator.aggregate_updates(updates)
            np.testing.assert_array_equal(result, expected)
            
        asyncio.run(run_test())

    def test_round_timeout_handling(self):
        # Simulate slow client
        async def slow_update():
            await asyncio.sleep(10)
            return {'weights': np.zeros(10), 'samples': 50}
        self.clients[0].get_model_update = slow_update
        
        with self.assertRaises(asyncio.TimeoutError):
            async def run_test():
                await self.coordinator.run_training_round(timeout=2)
                
            asyncio.run(run_test())

        self.mock_metrics.increment.assert_any_call(
            'fl.rounds.failed',
            tags={'reason': 'timeout'}
        )

    def test_client_selection_strategy(self):
        # Test different selection strategies
        strategies = [
            ('random', lambda x: x[:3]),
            ('stratified', lambda x: sorted(x, key=lambda c: c.id)[::2])
        ]
        
        for strategy_name, selection_fn in strategies:
            with self.subTest(strategy=strategy_name):
                self.coordinator.client_selection = selection_fn
                selected = self.coordinator.select_clients()
                self.assertEqual(len(selected), 3 if strategy_name == 'random' else 3)
                self.mock_metrics.increment.assert_any_call(
                    'fl.clients.selected',
                    tags={'strategy': strategy_name}
                )

if __name__ == '__main__':
    unittest.main()
