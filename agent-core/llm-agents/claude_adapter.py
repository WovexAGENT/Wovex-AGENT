import os
import json
import logging
import asyncio
from typing import Dict, List, Optional, Union, Any, AsyncGenerator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import wraps
import aiohttp
from pydantic import BaseModel, ValidationError, Field
from prometheus_client import Counter, Gauge, Histogram

# Metrics Configuration
CLAUDE_REQUEST_COUNTER = Counter(
    'claude_requests_total',
    'Total Claude API requests',
    ['model', 'endpoint', 'status']
)

CLAUDE_REQUEST_DURATION = Histogram(
    'claude_request_duration_seconds',
    'Claude API request duration',
    ['model', 'endpoint']
)

CLAUDE_TOKEN_USAGE = Gauge(
    'claude_token_usage',
    'Claude token usage',
    ['model', 'token_type']
)

@dataclass
class ClaudeAdapterConfig:
    api_key: str = field(default_factory=lambda: os.getenv("ANTHROPIC_API_KEY"))
    base_url: str = "https://api.anthropic.com/v1"
    default_model: str = "claude-3-opus-20240229"
    max_retries: int = 5
    request_timeout: int = 30
    max_connections: int = 10
    temperature: float = 0.7
    max_tokens: int = 1024
    system_prompt: str = "You are a helpful AI assistant."
    enable_streaming: bool = False
    rate_limit_delay: float = 0.1
    json_mode: bool = False

class ClaudeMessage(BaseModel):
    role: str = Field(..., pattern="^(user|assistant)$")
    content: Union[str, List[Dict[str, Any]]]

class ClaudeRequest(BaseModel):
    messages: List[ClaudeMessage]
    model: str = "claude-3-opus-20240229"
    system: Optional[str] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    stream: bool = False
    tools: Optional[List[Dict]] = None
    tool_choice: Optional[Union[str, Dict]] = None

class ClaudeResponse(BaseModel):
    id: str
    type: str
    role: str
    content: List[Dict]
    model: str
    stop_reason: Optional[str] = None
    stop_sequence: Optional[str] = None
    usage: Dict[str, int]

class ClaudeError(BaseModel):
    type: str
    message: str

class ClaudeAPIException(Exception):
    def __init__(self, status: int, error: ClaudeError):
        self.status = status
        self.error = error
        super().__init__(f"{error.type}: {error.message}")

class RateLimitExceeded(Exception):
    pass

def claude_metrics(f):
    @wraps(f)
    async def wrapper(self, *args, **kwargs):
        start_time = datetime.utcnow()
        try:
            result = await f(self, *args, **kwargs)
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            CLAUDE_REQUEST_DURATION.labels(
                model=kwargs.get('model', self.config.default_model),
                endpoint=f.__name__
            ).observe(duration)
            
            return result
        except Exception as e:
            CLAUDE_REQUEST_COUNTER.labels(
                model=kwargs.get('model', self.config.default_model),
                endpoint=f.__name__,
                status=e.__class__.__name__
            ).inc()
            raise
    return wrapper

class ClaudeAdapter:
    def __init__(self, config: ClaudeAdapterConfig):
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(config.max_connections)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._setup_validation()

    def _setup_validation(self):
        ClaudeRequest.model_rebuild()
        ClaudeResponse.model_rebuild()

    async def initialize(self):
        """Initialize connection pool"""
        self._session = aiohttp.ClientSession(
            base_url=self.config.base_url,
            headers={
                "x-api-key": self.config.api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            timeout=aiohttp.ClientTimeout(total=self.config.request_timeout)
        )
        self.logger.info("Claude adapter initialized")

    async def close(self):
        """Close connection pool"""
        if self._session:
            await self._session.close()
            self.logger.info("Claude adapter connection closed")

    @claude_metrics
    async def chat_completion(
        self,
        messages: List[Dict],
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        stream: Optional[bool] = None,
        tools: Optional[List[Dict]] = None,
        tool_choice: Optional[Union[str, Dict]] = None
    ) -> Union[ClaudeResponse, AsyncGenerator[ClaudeResponse, None]]:
        """
        Main method for Claude chat completions with:
        - Exponential backoff retry
        - Request validation
        - Token tracking
        - Error handling
        - Streaming support
        """
        model = model or self.config.default_model
        temperature = temperature or self.config.temperature
        max_tokens = max_tokens or self.config.max_tokens
        stream = stream if stream is not None else self.config.enable_streaming

        request_data = ClaudeRequest(
            messages=messages,
            model=model,
            system=self.config.system_prompt,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=stream,
            tools=tools,
            tool_choice=tool_choice
        ).model_dump(exclude_none=True)

        if self.config.json_mode:
            request_data["response_format"] = {"type": "json_object"}

        if stream:
            return self._stream_response(model, request_data)
        return await self._handle_request_with_retry(model, request_data)

    async def _handle_request_with_retry(
        self,
        model: str,
        request_data: Dict,
        attempt: int = 0
    ) -> ClaudeResponse:
        """Handle request with exponential backoff retry"""
        async with self._semaphore:
            try:
                async with self._session.post(
                    "/messages",
                    json=request_data
                ) as response:
                    CLAUDE_REQUEST_COUNTER.labels(
                        model=model,
                        endpoint="chat_completion",
                        status=response.status
                    ).inc()

                    if response.status == 429:
                        raise RateLimitExceeded()
                    
                    response.raise_for_status()
                    response_data = await response.json()

                    validated = ClaudeResponse(**response_data)
                    self._track_usage(model, validated.usage)
                    return validated

            except aiohttp.ClientResponseError as e:
                error_data = await self._parse_error_response(e)
                if attempt < self.config.max_retries and e.status >= 500:
                    return await self._retry_request(
                        model, request_data, attempt, e
                    )
                raise ClaudeAPIException(e.status, error_data)

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < self.config.max_retries:
                    return await self._retry_request(
                        model, request_data, attempt, e
                    )
                raise

    async def _retry_request(
        self,
        model: str,
        request_data: Dict,
        attempt: int,
        error: Exception
    ) -> ClaudeResponse:
        """Handle retry logic with exponential backoff"""
        delay = self._calculate_backoff(attempt)
        self.logger.warning(
            f"Retry {attempt + 1}/{self.config.max_retries} "
            f"after error: {str(error)}. Waiting {delay} seconds"
        )
        await asyncio.sleep(delay)
        return await self._handle_request_with_retry(
            model, request_data, attempt + 1
        )

    def _calculate_backoff(self, attempt: int) -> float:
        """Exponential backoff with jitter"""
        base_delay = min(2 ** attempt, 60)
        jitter = base_delay * 0.1
        return base_delay + jitter

    async def _parse_error_response(self, error: aiohttp.ClientResponseError) -> ClaudeError:
        """Parse error response from API"""
        try:
            error_data = await error.response.json()
            return ClaudeError(**error_data.get("error", {}))
        except (json.JSONDecodeError, ValidationError):
            return ClaudeError(
                type="unknown_error",
                message="Unable to parse error response"
            )

    def _track_usage(self, model: str, usage: Dict[str, int]):
        """Update token usage metrics"""
        for token_type, count in usage.items():
            CLAUDE_TOKEN_USAGE.labels(
                model=model,
                token_type=token_type
            ).set(count)

    async def _stream_response(self, model: str, request_data: Dict) -> AsyncGenerator[ClaudeResponse, None]:
        """Handle streaming response"""
        async with self._session.post(
            "/messages",
            json=request_data
        ) as response:
            response.raise_for_status()
            
            buffer = ""
            async for chunk in response.content:
                buffer += chunk.decode()
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if line.startswith("data: "):
                        data = line[6:].strip()
                        if data == "[DONE]":
                            return
                        try:
                            response_data = json.loads(data)
                            validated = ClaudeResponse(**response_data)
                            self._track_usage(model, validated.usage)
                            yield validated
                        except (json.JSONDecodeError, ValidationError) as e:
                            self.logger.error(f"Invalid stream data: {str(e)}")

    def format_tool_message(self, tool_call_id: str, content: str) -> Dict:
        """Format tool result message for Claude"""
        return {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": tool_call_id,
                    "content": content
                }
            ]
        }

    def format_tool_call(self, tool_call: Dict) -> Dict:
        """Format tool call message for Claude"""
        return {
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": tool_call["id"],
                    "name": tool_call["name"],
                    "input": tool_call["arguments"]
                }
            ]
        }

class ClaudeAdapterFactory:
    @staticmethod
    def create_from_env() -> ClaudeAdapter:
        """Factory method to create adapter from environment variables"""
        return ClaudeAdapter(ClaudeAdapterConfig())

# Example Usage
async def example_usage():
    config = ClaudeAdapterConfig(
        api_key="your_api_key",
        default_model="claude-3-sonnet-20240229"
    )
    
    adapter = ClaudeAdapter(config)
    await adapter.initialize()
    
    try:
        messages = [
            {"role": "user", "content": "Explain quantum computing in 50 words"}
        ]
        
        response = await adapter.chat_completion(messages)
        print(f"Response: {response.content[0]['text']}")
        
    finally:
        await adapter.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(example_usage())
