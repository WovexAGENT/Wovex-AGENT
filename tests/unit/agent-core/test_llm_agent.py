import unittest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import datetime
from dataclasses import dataclass
from typing import Any, Dict, Optional

from llm_agent import LLMAgent, LLMConfig, LLMRequest, LLMResponse, LLMError

@dataclass
class MockLLMClient:
    generate: AsyncMock = AsyncMock()
    stream_generate: AsyncMock = AsyncMock()

class TestLLMAgent(unittest.IsolatedAsyncioTestCase):
    
    def setUp(self):
        self.mock_metrics = MagicMock()
        self.mock_cache = AsyncMock()
        self.mock_auth = MagicMock()
        
        self.llm_config = LLMConfig(
            model_name="gpt-4",
            max_retries=3,
            timeout=5.0,
            temperature=0.7,
            max_tokens=1000,
            api_key="test_key"
        )
        
        self.agent = LLMAgent(
            config=self.llm_config,
            metrics_client=self.mock_metrics,
            cache_client=self.mock_cache,
            auth_provider=self.mock_auth
        )
        
        self.mock_client = MockLLMClient()
        self.agent._client = self.mock_client

    async def asyncTearDown(self):
        await self.agent.close()

    async def test_successful_text_generation(self):
        test_request = LLMRequest(
            prompt="Explain quantum computing",
            temperature=0.5,
            max_tokens=500
        )
        
        mock_response = LLMResponse(
            content="Quantum computing leverages quantum mechanics...",
            model="gpt-4",
            created_at=datetime.now(),
            usage={"prompt_tokens": 20, "completion_tokens": 150}
        )
        
        self.mock_client.generate.return_value = mock_response
        
        result = await self.agent.process_request(test_request)
        
        self.mock_client.generate.assert_awaited_once()
        self.assertEqual(result.content, mock_response.content)
        self.mock_metrics.timing.assert_called_once_with("llm.latency", unittest.mock.ANY)

    async def test_request_retry_mechanism(self):
        test_request = LLMRequest(prompt="Test retry mechanism")
        
        self.mock_client.generate.side_effect = [
            LLMError("Temporary failure", code=500),
            LLMError("Temporary failure", code=503),
            LLMResponse(content="Success after retries")
        ]
        
        result = await self.agent.process_request(test_request)
        
        self.assertEqual(self.mock_client.generate.call_count, 3)
        self.assertEqual(result.content, "Success after retries")
        self.mock_metrics.increment.assert_has_calls([
            call("llm.retries", 1),
            call("llm.retries", 1)
        ])

    async def test_rate_limit_handling(self):
        test_request = LLMRequest(prompt="Test rate limiting")
        
        self.mock_client.generate.side_effect = LLMError(
            "Rate limit exceeded", 
            code=429,
            headers={"Retry-After": "5"}
        )
        
        with self.assertRaises(LLMError):
            await self.agent.process_request(test_request)
            
        self.mock_metrics.increment.assert_called_with("llm.rate_limited")

    async def test_streaming_generation(self):
        test_request = LLMRequest(
            prompt="Streaming test",
            stream=True
        )
        
        mock_stream = [
            LLMResponse(content="Chunk1 ", is_partial=True),
            LLMResponse(content="Chunk2 ", is_partial=True),
            LLMResponse(content="Final chunk", is_partial=False)
        ]
        
        self.mock_client.stream_generate.return_value = mock_stream
        
        collected = []
        async for chunk in self.agent.stream_request(test_request):
            collected.append(chunk.content)
            
        self.assertEqual(collected, ["Chunk1 ", "Chunk2 ", "Final chunk"])
        self.mock_metrics.increment.assert_called_with("llm.stream_requests")

    async def test_input_validation(self):
        with self.assertRaises(ValueError):
            await self.agent.process_request(LLMRequest(prompt=""))
            
        with self.assertRaises(ValueError):
            await self.agent.process_request(LLMRequest(prompt="A" * 10000))

    async def test_auth_injection(self):
        test_request = LLMRequest(prompt="Auth test")
        self.mock_auth.get_credentials.return_value = {"Authorization": "Bearer valid_token"}
        
        await self.agent.process_request(test_request)
        
        call_args = self.mock_client.generate.call_args[1]
        self.assertEqual(call_args["headers"]["Authorization"], "Bearer valid_token")

    async def test_cache_integration(self):
        test_request = LLMRequest(prompt="Cache test")
        cached_response = LLMResponse(content="Cached response")
        
        self.mock_cache.get.return_value = cached_response
        
        result = await self.agent.process_request(test_request)
        
        self.mock_cache.get.assert_awaited_once()
        self.assertEqual(result.content, "Cached response")
        self.mock_client.generate.assert_not_called()

    async def test_sensitive_data_filtering(self):
        test_request = LLMRequest(
            prompt="My API key is SECRET123",
            sanitize=True
        )
        
        await self.agent.process_request(test_request)
        
        submitted_prompt = self.mock_client.generate.call_args[0][0].prompt
        self.assertNotIn("SECRET123", submitted_prompt)

    async def test_cost_calculation(self):
        test_request = LLMRequest(prompt="Cost calculation test")
        mock_response = LLMResponse(
            content="Test response",
            usage={"prompt_tokens": 100, "completion_tokens": 200}
        )
        
        self.mock_client.generate.return_value = mock_response
        await self.agent.process_request(test_request)
        
        self.mock_metrics.record_cost.assert_called_once_with(
            "gpt-4",
            100,
            200
        )

    async def test_config_validation(self):
        with self.assertRaises(ValueError):
            LLMAgent(config=LLMConfig(model_name="invalid"))
            
        with self.assertRaises(ValueError):
            LLMAgent(config=LLMConfig(
                model_name="gpt-4",
                temperature=2.0
            ))

    async def test_connection_pooling(self):
        with patch('llm_agent.LLMClient') as mock_client:
            agent = LLMAgent(config=self.llm_config)
            
            # First request
            await agent.process_request(LLMRequest(prompt="Test 1"))
            # Second request
            await agent.process_request(LLMRequest(prompt="Test 2"))
            
            mock_client.assert_called_once()
            self.assertEqual(mock_client.return_value.generate.call_count, 2)

    async def test_timeout_handling(self):
        test_request = LLMRequest(prompt="Timeout test")
        
        async def slow_response():
            await asyncio.sleep(10)
            return LLMResponse(content="Too late")
            
        self.mock_client.generate.side_effect = slow_response
        
        with self.assertRaises(LLMError) as context:
            await self.agent.process_request(test_request)
            
        self.assertEqual(context.exception.code, 504)
        self.mock_metrics.increment.assert_called_with("llm.timeouts")

if __name__ == "__main__":
    unittest.main()
