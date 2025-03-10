import os
import json
import logging
import asyncio
from typing import Dict, List, Optional, Union, Any, AsyncGenerator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import wraps
import aiohttp
from pydantic import BaseModel, ValidationError, Field, validator
from prometheus_client import Counter, Gauge, Histogram

# Metrics Configuration
GPT4_REQUEST_COUNTER = Counter(
    'gpt4_requests_total',
    'Total GPT-4 API requests',
    ['model', 'endpoint', 'status']
)

GPT4_REQUEST_DURATION = Histogram(
    'gpt4_request_duration_seconds',
    'GPT-4 API request duration',
    ['model', 'endpoint']
)

GPT4_TOKEN_USAGE = Gauge(
    'gpt4_token_usage',
    'GPT-4 token usage',
    ['model', 'token_type']
)

@dataclass
class GPT4AdapterConfig:
    api_key: str = field(default_factory=lambda: os.getenv("OPENAI_API_KEY"))
    base_url: str = "https://api.openai.com/v1"
    default_model: str = "gpt-4-0613"
    max_retries: int = 5
    request_timeout: int = 30
    max_connections: int = 10
    temperature: float = 0.7
    max_tokens: int = 1024
    system_message: str = "You are a helpful AI assistant."
    enable_streaming: bool = False
    rate_limit_delay: float = 0.1
    json_mode: bool = False
    frequency_penalty: float = 0.0
    presence_penalty: float = 0.0

class ChatMessage(BaseModel):
    role: str = Field(..., pattern="^(system|user|assistant|tool)$")
    content: str
    name: Optional[str] = None
    tool_call_id: Optional[str] = None

class ToolCall(BaseModel):
    id: str
    type: str = "function"
    function: Dict[str, Any]

class GPT4Request(BaseModel):
    messages: List[ChatMessage]
    model: str = "gpt-4-0613"
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    stream: bool = False
    tools: Optional[List[Dict]] = None
    tool_choice: Optional[Union[str, Dict]] = None
    response_format: Optional[Dict] = None

class CompletionUsage(BaseModel):
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int

class GPT4Response(BaseModel):
    id: str
    object: str
    created: int
    model: str
    choices: List[Dict]
    usage: Optional[CompletionUsage] = None

class OpenAIError(BaseModel):
    code: Optional[str] = None
    message: str
    param: Optional[str] = None
    type: str

class GPT4APIException(Exception):
    def __init__(self, status: int, error: OpenAIError):
        self.status = status
        self.error = error
        super().__init__(f"{error.type}: {error.message}")

class RateLimitExceeded(Exception):
    pass

def gpt4_metrics(f):
    @wraps(f)
    async def wrapper(self, *args, **kwargs):
        start_time = datetime.utcnow()
        try:
            result = await f(self, *args, **kwargs)
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            GPT4_REQUEST_DURATION.labels(
                model=kwargs.get('model', self.config.default_model),
                endpoint=f.__name__
            ).observe(duration)
            
            return result
        except Exception as e:
            GPT4_REQUEST_COUNTER.labels(
                model=kwargs.get('model', self.config.default_model),
                endpoint=f.__name__,
                status=e.__class__.__name__
            ).inc()
            raise
    return wrapper

class GPT4Adapter:
    def __init__(self, config: GPT4AdapterConfig):
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(config.max_connections)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._setup_validation()

    def _setup_validation(self):
        GPT4Request.model_rebuild()
        GPT4Response.model_rebuild()

    async def initialize(self):
        """Initialize connection pool"""
        self._session = aiohttp.ClientSession(
            base_url=self.config.base_url,
            headers={
                "Authorization": f"Bearer {self.config.api_key}",
                "Content-Type": "application/json"
            },
            timeout=aiohttp.ClientTimeout(total=self.config.request_timeout)
        )
        self.logger.info("GPT-4 adapter initialized")

    async def close(self):
        """Close connection pool"""
        if self._session:
            await self._session.close()
            self.logger.info("GPT-4 adapter connection closed")

    @gpt4_metrics
    async def chat_completion(
        self,
        messages: List[Dict],
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        stream: Optional[bool] = None,
        tools: Optional[List[Dict]] = None,
        tool_choice: Optional[Union[str, Dict]] = None
    ) -> Union[GPT4Response, AsyncGenerator[GPT4Response, None]]:
        """
        Main method for GPT-4 chat completions with:
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

        request_data = GPT4Request(
            messages=messages,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=stream,
            tools=tools,
            tool_choice=tool_choice,
            response_format={"type": "json_object"} if self.config.json_mode else None
        ).model_dump(exclude_none=True)

        if self.config.system_message:
            request_data['messages'].insert(0, {
                "role": "system",
                "content": self.config.system_message
            })

        if stream:
            return self._stream_response(model, request_data)
        return await self._handle_request_with_retry(model, request_data)

    async def _handle_request_with_retry(
        self,
        model: str,
        request_data: Dict,
        attempt: int = 0
    ) -> GPT4Response:
        """Handle request with exponential backoff retry"""
        async with self._semaphore:
            try:
                async with self._session.post(
                    "/chat/completions",
                    json=request_data
                ) as response:
                    GPT4_REQUEST_COUNTER.labels(
                        model=model,
                        endpoint="chat_completion",
                        status=response.status
                    ).inc()

                    if response.status == 429:
                        raise RateLimitExceeded()
                    
                    response.raise_for_status()
                    response_data = await response.json()

                    validated = GPT4Response(**response_data)
                    self._track_usage(model, validated.usage)
                    return validated

            except aiohttp.ClientResponseError as e:
                error_data = await self._parse_error_response(e)
                if attempt < self.config.max_retries and e.status >= 500:
                    return await self._retry_request(
                        model, request_data, attempt, e
                    )
                raise GPT4APIException(e.status, error_data)

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
    ) -> GPT4Response:
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

    async def _parse_error_response(self, error: aiohttp.ClientResponseError) -> OpenAIError:
        """Parse error response from API"""
        try:
            error_data = await error.response.json()
            return OpenAIError(**error_data.get("error", {}))
        except (json.JSONDecodeError, ValidationError):
            return OpenAIError(
                type="unknown_error",
                message="Unable to parse error response"
            )

    def _track_usage(self, model: str, usage: Optional[CompletionUsage]):
        """Update token usage metrics"""
        if usage:
            GPT4_TOKEN_USAGE.labels(
                model=model,
                token_type="prompt"
            ).set(usage.prompt_tokens)
            
            GPT4_TOKEN_USAGE.labels(
                model=model,
                token_type="completion"
            ).set(usage.completion_tokens)

    async def _stream_response(self, model: str, request_data: Dict) -> AsyncGenerator[GPT4Response, None]:
        """Handle streaming response"""
        async with self._session.post(
            "/chat/completions",
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
                            validated = GPT4Response(**response_data)
                            yield validated
                        except (json.JSONDecodeError, ValidationError) as e:
                            self.logger.error(f"Invalid stream data: {str(e)}")

    def format_tool_message(self, tool_call_id: str, content: str) -> Dict:
        """Format tool result message for GPT-4"""
        return {
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": content
        }

    def format_tool_call(self, tool_call: Dict) -> Dict:
        """Format tool call message for GPT-4"""
        return {
            "role": "assistant",
            "content": "",
            "tool_calls": [tool_call]
        }

class GPT4AdapterFactory:
    @staticmethod
    def create_from_env() -> 'GPT4Adapter':
        """Factory method to create adapter from environment variables"""
        return GPT4Adapter(GPT4AdapterConfig())

# Example Usage
async def example_usage():
    config = GPT4AdapterConfig(
        api_key="your_api_key",
        default_model="gpt-4-turbo"
    )
    
    adapter = GPT4Adapter(config)
    await adapter.initialize()
    
    try:
        messages = [
            {"role": "user", "content": "Explain quantum computing in 50 words"}
        ]
        
        response = await adapter.chat_completion(messages)
        if response.choices:
            print(f"Response: {response.choices[0].message['content']}")
        
    finally:
        await adapter.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(example_usage())
