import os
import logging
import hashlib
import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
    Callable,
    AsyncGenerator
)
import json
import tempfile
from concurrent.futures import ThreadPoolExecutor

import torch
import numpy as np
from pydantic import BaseModel, ValidationError
from prometheus_client import Counter, Gauge, Histogram

# Configuration Models
class ModelConfig(BaseModel):
    name: str
    version: str
    framework: str
    storage_uri: str
    min_memory_mb: int = 1024
    max_batch_size: int = 32
    required_components: List[str] = []
    metadata: Dict[str, Any] = {}

# Data Models
@dataclass
class ModelMetadata:
    name: str
    versions: Dict[str, 'ModelVersion']
    active_version: str
    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any]

@dataclass
class ModelVersion:
    config: ModelConfig
    loaded: bool = False
    load_time: float = 0.0
    performance: 'PerformanceMetrics' = field(default_factory=lambda: PerformanceMetrics())

@dataclass
class PerformanceMetrics:
    inference_count: int = 0
    avg_latency: float = 0.0
    error_count: int = 0
    last_used: float = 0.0

# Exceptions
class ModelError(Exception): pass
class ModelLoadingError(ModelError): pass
class InferenceError(ModelError): pass
class ModelNotFoundError(ModelError): pass
class InvalidInputError(ModelError): pass
class VersionConflictError(ModelError): pass

# Metrics
MODEL_LOAD_COUNTER = Counter('model_load_total', 'Total model loads', ['model', 'version'])
MODEL_UNLOAD_COUNTER = Counter('model_unload_total', 'Total model unloads', ['model', 'version'])
INFERENCE_COUNTER = Counter('model_inference_total', 'Total inference requests', ['model', 'version'])
INFERENCE_LATENCY = Histogram('model_inference_latency_seconds', 'Inference latency distribution', ['model', 'version'])
CACHE_GAUGE = Gauge('model_cache_size', 'Current number of loaded models')
MEMORY_GAUGE = Gauge('model_memory_usage_mb', 'Memory used by models', ['model', 'version'])

class ModelManager:
    def __init__(
        self,
        cache_size: int = 10,
        models_dir: Path = Path("models"),
        max_workers: int = 4,
        download_timeout: int = 300
    ):
        self.cache_size = cache_size
        self.models_dir = models_dir
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.download_timeout = download_timeout
        
        self._model_cache: Dict[str, ModelMetadata] = {}
        self._version_cache: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
        
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger("ModelManager")
        self.logger.setLevel(logging.INFO)

    async def load_model(self, config: ModelConfig) -> ModelMetadata:
        """Load a model into memory with validation and version control"""
        async with self._lock:
            model_id = f"{config.name}-{config.version}"
            
            if model_id in self._version_cache:
                self.logger.info(f"Model {model_id} already loaded")
                return self._model_cache[config.name]
            
            try:
                # Validate system resources
                if not await self._check_resources(config):
                    raise ModelLoadingError("Insufficient system resources")
                
                # Download model artifacts
                model_path = await self._download_model(config)
                
                # Load model into memory
                start_time = time.time()
                model = await self._load_model_framework(config, model_path)
                load_time = time.time() - start_time
                
                # Initialize metadata
                metadata = ModelMetadata(
                    name=config.name,
                    versions={config.version: ModelVersion(config)},
                    active_version=config.version,
                    input_schema=await self._get_input_schema(model),
                    output_schema=await self._get_output_schema(model)
                )
                
                # Update caches
                self._version_cache[model_id] = model
                self._model_cache[config.name] = metadata
                metadata.versions[config.version].loaded = True
                metadata.versions[config.version].load_time = load_time
                
                MODEL_LOAD_COUNTER.labels(config.name, config.version).inc()
                MEMORY_GAUGE.labels(config.name, config.version).set(config.min_memory_mb)
                CACHE_GAUGE.set(len(self._version_cache))
                
                return metadata
            except Exception as e:
                self.logger.error(f"Failed to load model {model_id}: {str(e)}")
                raise ModelLoadingError(f"Model loading failed: {str(e)}") from e

    async def unload_model(self, model_name: str, version: Optional[str] = None):
        """Unload model from memory with version support"""
        async with self._lock:
            if version:
                model_id = f"{model_name}-{version}"
                if model_id in self._version_cache:
                    del self._version_cache[model_id]
                    MODEL_UNLOAD_COUNTER.labels(model_name, version).inc()
            else:
                versions_to_remove = [k for k in self._version_cache if k.startswith(f"{model_name}-")]
                for model_id in versions_to_remove:
                    _, ver = model_id.split("-", 1)
                    del self._version_cache[model_id]
                    MODEL_UNLOAD_COUNTER.labels(model_name, ver).inc()
            
            CACHE_GAUGE.set(len(self._version_cache))
            MEMORY_GAUGE.remove(model_name, version or 'all')

    async def predict(
        self,
        model_name: str,
        input_data: Any,
        version: Optional[str] = None,
        batch_size: int = 1
    ) -> Any:
        """Perform inference with versioning and batching support"""
        start_time = time.time()
        model_id = await self._resolve_model_version(model_name, version)
        
        try:
            model = self._version_cache[model_id]
            metadata = self._model_cache[model_name]
            
            # Validate input against schema
            if not self._validate_input(input_data, metadata.input_schema):
                raise InvalidInputError("Input data schema mismatch")
            
            # Batch processing
            if batch_size > 1:
                results = []
                for batch in self._create_batches(input_data, batch_size):
                    results.extend(await self._run_inference(model, batch))
            else:
                results = await self._run_inference(model, input_data)
            
            # Update performance metrics
            latency = time.time() - start_time
            self._update_metrics(model_name, metadata.active_version, latency)
            
            return results
        except Exception as e:
            self.logger.error(f"Inference failed for {model_id}: {str(e)}")
            INFERENCE_COUNTER.labels(model_name, version or 'latest').inc()
            raise InferenceError(f"Inference error: {str(e)}") from e

    async def stream_predict(
        self,
        model_name: str,
        input_stream: AsyncGenerator[Any, None],
        version: Optional[str] = None
    ) -> AsyncGenerator[Any, None]:
        """Streaming inference interface"""
        model_id = await self._resolve_model_version(model_name, version)
        model = self._version_cache[model_id]
        
        async for chunk in input_stream:
            try:
                result = await self.predict(model_name, chunk, version)
                yield result
            except InferenceError as e:
                self.logger.error(f"Stream inference error: {str(e)}")
                yield {"error": str(e)}

    # Private implementation details
    async def _download_model(self, config: ModelConfig) -> Path:
        """Download model artifacts from storage"""
        download_path = self.models_dir / config.name / config.version
        
        if download_path.exists():
            self.logger.info(f"Using cached model at {download_path}")
            return download_path
        
        try:
            self.logger.info(f"Downloading model {config.name} version {config.version}")
            download_path.mkdir(parents=True, exist_ok=True)
            
            # Implement actual download logic here
            # Example: await self._download_from_s3(config.storage_uri, download_path)
            
            return download_path
        except Exception as e:
            raise ModelLoadingError(f"Download failed: {str(e)}") from e

    async def _load_model_framework(self, config: ModelConfig, path: Path) -> Any:
        """Load model using appropriate framework"""
        try:
            if config.framework == "pytorch":
                return torch.jit.load(str(path / "model.pt"))
            elif config.framework == "huggingface":
                from transformers import AutoModel, AutoTokenizer
                model = AutoModel.from_pretrained(str(path))
                tokenizer = AutoTokenizer.from_pretrained(str(path))
                return {"model": model, "tokenizer": tokenizer}
            else:
                raise ModelLoadingError(f"Unsupported framework: {config.framework}")
        except Exception as e:
            raise ModelLoadingError(f"Framework loading error: {str(e)}") from e

    async def _check_resources(self, config: ModelConfig) -> bool:
        """Verify system resources before loading"""
        # Implement actual resource checks
        return True

    def _validate_input(self, input_data: Any, schema: Dict[str, Any]) -> bool:
        """Validate input data against model schema"""
        # Implement schema validation logic
        return True

    async def _resolve_model_version(self, model_name: str, version: Optional[str]) -> str:
        """Resolve model version from alias or latest"""
        if model_name not in self._model_cache:
            raise ModelNotFoundError(f"Model {model_name} not found")
            
        metadata = self._model_cache[model_name]
        version = version or metadata.active_version
        model_id = f"{model_name}-{version}"
        
        if model_id not in self._version_cache:
            raise ModelNotFoundError(f"Version {version} not loaded for {model_name}")
            
        return model_id

    def _update_metrics(self, model: str, version: str, latency: float):
        """Update performance metrics and statistics"""
        INFERENCE_COUNTER.labels(model, version).inc()
        INFERENCE_LATENCY.labels(model, version).observe(latency)
        
        metadata = self._model_cache[model]
        version_data = metadata.versions[version]
        version_data.performance.inference_count += 1
        version_data.performance.avg_latency = (
            (version_data.performance.avg_latency * (version_data.performance.inference_count - 1) + latency) 
            / version_data.performance.inference_count
        )
        version_data.performance.last_used = time.time()

    def _create_batches(self, data: Any, batch_size: int) -> List[Any]:
        """Create inference batches from input data"""
        # Implement batching logic based on data type
        return [data]

    async def _run_inference(self, model: Any, input_data: Any) -> Any:
        """Execute model inference with framework-specific handling"""
        try:
            if isinstance(model, dict) and "tokenizer" in model:
                # Hugging Face specific processing
                inputs = model["tokenizer"](input_data, return_tensors="pt")
                outputs = model["model"](**inputs)
                return outputs.last_hidden_state.detach().numpy()
            else:
                # Generic PyTorch inference
                with torch.no_grad():
                    return model(input_data)
        except Exception as e:
            raise InferenceError(f"Inference runtime error: {str(e)}") from e

    # Additional management features
    async def warmup_model(self, model_name: str, version: str, sample_input: Any):
        """Warm up model with sample data"""
        try:
            await self.predict(model_name, sample_input, version)
            self.logger.info(f"Successfully warmed up {model_name} version {version}")
        except InferenceError as e:
            self.logger.warning(f"Warmup failed for {model_name}: {str(e)}")

    async def health_check(self, model_name: str, version: str) -> Dict[str, Any]:
        """Perform comprehensive health check on loaded model"""
        model_id = f"{model_name}-{version}"
        if model_id not in self._version_cache:
            return {"healthy": False, "error": "Model not loaded"}
        
        try:
            # Implement framework-specific checks
            return {"healthy": True, "inference_count": self._model_cache[model_name].versions[version].performance.inference_count}
        except Exception as e:
            return {"healthy": False, "error": str(e)}

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    async def main():
        manager = ModelManager(cache_size=5)
        
        # Example model configuration
        config = ModelConfig(
            name="bert-base",
            version="1.0",
            framework="huggingface",
            storage_uri="s3://models/bert-base-1.0",
            min_memory_mb=2048
        )
        
        # Load model
        await manager.load_model(config)
        
        # Sample inference
        sample_input = "Hello world!"
        result = await manager.predict("bert-base", sample_input)
        print(f"Inference result: {result}")
        
        # Batch processing
        batch_input = [f"Text {i}" for i in range(10)]
        batch_results = await manager.predict("bert-base", batch_input, batch_size=4)
        print(f"Batch results: {len(batch_results)}")
        
        # Cleanup
        await manager.unload_model("bert-base")

    asyncio.run(main())
