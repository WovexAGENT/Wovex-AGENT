import unittest
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
import grpc
from grpc_testing import server_from_dictionary, services

# Protobuf/gRPC imports
import coordinator_pb2
import coordinator_pb2_grpc
from server import CoordinatorService

class TestCoordinatorService(unittest.TestCase):
    
    def setUp(self):
        self.mock_store = MagicMock()
        self.mock_metrics = MagicMock()
        self.mock_auth = MagicMock()
        
        # Create test server
        self.service = CoordinatorService(
            store=self.mock_store,
            metrics=self.mock_metrics,
            auth_provider=self.mock_auth
        )
        
        # Setup testing environment
        self.test_server = server_from_dictionary({
            coordinator_pb2.DESCRIPTOR.services_by_name['Coordinator']: self.service
        })

    def tearDown(self):
        self.mock_store.reset_mock()
        self.mock_metrics.reset_mock()

    def _invoke_unary(self, method, request):
        return self.test_server.invoke_unary_unary(
            method_descriptor=method,
            invocation_metadata={},
            request=request,
            timeout=1
        )

    def test_node_registration_success(self):
        request = coordinator_pb2.NodeRegistrationRequest(
            node_id="test-node",
            capabilities=["processing", "storage"],
            endpoint="node.example.com:50051"
        )
        
        self.mock_auth.verify_credentials.return_value = True
        self.mock_store.register_node.return_value = coordinator_pb2.NodeRegistrationResponse(
            assigned_id="node-123",
            auth_token="generated-token",
            heartbeat_interval=30
        )
        
        response, metadata, code, details = self._invoke_unary(
            coordinator_pb2.DESCRIPTOR.services_by_name['Coordinator'].methods_by_name['RegisterNode'],
            request
        )
        
        self.assertEqual(code, grpc.StatusCode.OK)
        self.assertEqual(response.assigned_id, "node-123")
        self.mock_metrics.increment.assert_called_with("node.registrations")

    def test_invalid_authentication(self):
        request = coordinator_pb2.TaskRequest(task_id="task-123")
        self.mock_auth.verify_credentials.return_value = False
        
        _, _, code, _ = self._invoke_unary(
            coordinator_pb2.DESCRIPTOR.services_by_name['Coordinator'].methods_by_name['SubmitTask'],
            request
        )
        
        self.assertEqual(code, grpc.StatusCode.UNAUTHENTICATED)

    def test_task_processing_flow(self):
        self.mock_store.create_task.return_value = coordinator_pb2.TaskResponse(
            task_id="task-456",
            status=coordinator_pb2.TaskStatus.PENDING
        )
        
        request = coordinator_pb2.TaskRequest(
            payload="test payload".encode(),
            priority=coordinator_pb2.PRIORITY_HIGH
        )
        
        response, _, code, _ = self._invoke_unary(
            coordinator_pb2.DESCRIPTOR.services_by_name['Coordinator'].methods_by_name['SubmitTask'],
            request
        )
        
        self.assertEqual(code, grpc.StatusCode.OK)
        self.assertEqual(response.status, coordinator_pb2.TaskStatus.PENDING)
        self.mock_metrics.timing.assert_called_with("task.processing_latency", unittest.mock.ANY)

    @patch('server.CoordinatorService._validate_task')
    def test_invalid_task_handling(self, mock_validate):
        mock_validate.return_value = False
        request = coordinator_pb2.TaskRequest()
        
        _, _, code, details = self._invoke_unary(
            coordinator_pb2.DESCRIPTOR.services_by_name['Coordinator'].methods_by_name['SubmitTask'],
            request
        )
        
        self.assertEqual(code, grpc.StatusCode.INVALID_ARGUMENT)
        self.assertIn("Invalid task", details)

    def test_heartbeat_processing(self):
        request = coordinator_pb2.Heartbeat(
            node_id="node-123",
            timestamp=int(datetime.now().timestamp())
        )
        
        self.mock_store.update_heartbeat.return_value = coordinator_pb2.HeartbeatAck(
            required_action=coordinator_pb2.NO_ACTION
        )
        
        response, _, code, _ = self._invoke_unary(
            coordinator_pb2.DESCRIPTOR.services_by_name['Coordinator'].methods_by_name['ProcessHeartbeat'],
            request
        )
        
        self.assertEqual(code, grpc.StatusCode.OK)
        self.assertEqual(response.required_action, coordinator_pb2.NO_ACTION)
        self.mock_metrics.increment.assert_called_with("heartbeat.received")

    def test_high_concurrency_handling(self):
        from concurrent.futures import ThreadPoolExecutor
        
        request = coordinator_pb2.TaskRequest()
        self.mock_store.create_task.return_value = coordinator_pb2.TaskResponse()
        
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(
                self._invoke_unary,
                coordinator_pb2.DESCRIPTOR.services_by_name['Coordinator'].methods_by_name['SubmitTask'],
                request
            ) for _ in range(100)]
            
            results = [f.result() for f in futures]
        
        self.assertTrue(all(r[2] == grpc.StatusCode.OK for r in results))
        self.assertEqual(self.mock_metrics.timing.call_count, 100)

    def test_streaming_metrics_collection(self):
        # Setup streaming test
        request = coordinator_pb2.MetricsQuery()
        self.mock_store.stream_metrics.return_value = iter([
            coordinator_pb2.MetricEntry(metric="cpu_usage", value=0.75),
            coordinator_pb2.MetricEntry(metric="memory_usage", value=0.65)
        ])
        
        # Execute streaming RPC
        responses = list(self.test_server.invoke_stream_unary(
            method_descriptor=coordinator_pb2.DESCRIPTOR.services_by_name['Coordinator'].methods_by_name['StreamMetrics'],
            invocation_metadata=[],
            request=request,
            timeout=1
        ).responses)
        
        self.assertEqual(len(responses), 2)
        self.assertEqual(responses[0].metric, "cpu_usage")

    def test_deadline_exceeded_handling(self):
        request = coordinator_pb2.ComplexQuery()
        self.mock_store.process_query.side_effect = asyncio.sleep(2)
        
        _, _, code, _ = self._invoke_unary(
            coordinator_pb2.DESCRIPTOR.services_by_name['Coordinator'].methods_by_name['ExecuteComplexQuery'],
            request
        )
        
        self.assertEqual(code, grpc.StatusCode.DEADLINE_EXCEEDED)

    def test_server_metadata_handling(self):
        request = coordinator_pb2.NodeRegistrationRequest()
        self.mock_auth.verify_credentials.return_value = True
        
        response, metadata, code, _ = self.test_server.invoke_unary_unary(
            method_descriptor=coordinator_pb2.DESCRIPTOR.services_by_name['Coordinator'].methods_by_name['RegisterNode'],
            invocation_metadata=[('x-client-version', '1.4.0')],
            request=request,
            timeout=1
        )
        
        self.mock_store.register_node.assert_called_with(
            unittest.mock.ANY,
            client_version='1.4.0'
        )

    def test_tls_configuration_validation(self):
        from test_server import load_credentials
        cert, key = load_credentials()
        self.assertIsNotNone(cert)
        self.assertIsNotNone(key)
        self.assertTrue(len(cert) > 1000)
        self.assertTrue(len(key) > 1000)

if __name__ == '__main__':
    unittest.main()
