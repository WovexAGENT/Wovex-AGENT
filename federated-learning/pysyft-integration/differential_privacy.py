import logging
import math
import numpy as np
import scipy.stats as stats
from typing import List, Tuple, Union, Dict, Any, Optional, Callable
from pydantic import BaseModel, ValidationError, validator
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from collections import defaultdict
from prometheus_client import Counter, Histogram

# Metrics
DP_EPSILON_USAGE = Counter('dp_epsilon_consumed', 'Total epsilon privacy budget consumed')
DP_DELTA_USAGE = Counter('dp_delta_consumed', 'Total delta privacy budget consumed')
DP_QUERY_COUNT = Counter('dp_queries_total', 'Total differential privacy queries')
DP_NOISE_AMOUNT = Histogram('dp_noise_added', 'Magnitude of added noise')

class PrivacyError(Exception):
    """Base exception for differential privacy failures"""

class BudgetExhaustedError(PrivacyError):
    """Raised when privacy budget is exhausted"""

class InvalidParametersError(PrivacyError):
    """Invalid mechanism parameters"""

class SensitivityError(PrivacyError):
    """Invalid sensitivity calculation"""

class DPMechanism(Enum):
    LAPLACE = "laplace"
    GAUSSIAN = "gaussian"
    EXPONENTIAL = "exponential"
    RANDOM_RESPONSE = "random_response"

class PrivacyRequest(BaseModel):
    mechanism: DPMechanism
    epsilon: float = 0.5
    delta: float = 1e-5
    sensitivity: float = 1.0
    max_retries: int = 3
    max_value: Optional[float] = None
    min_value: Optional[float] = None
    dataset_size: Optional[int] = None
    custom_score_fn: Optional[Callable] = None

    @validator('epsilon')
    def epsilon_positive(cls, v):
        if v <= 0:
            raise ValueError("Epsilon must be positive")
        return v

    @validator('delta')
    def delta_range(cls, v, values):
        if 'mechanism' in values and values['mechanism'] == DPMechanism.GAUSSIAN:
            if v <= 0 or v >= 1:
                raise ValueError("Delta must be in (0,1) for Gaussian mechanism")
        return v

@dataclass
class PrivacyAccountant:
    total_epsilon: float = 0.0
    total_delta: float = 0.0
    max_epsilon: Optional[float] = None
    max_delta: Optional[float] = None
    composition: str = "basic"
    budget_history: List[Tuple[float, float]] = field(default_factory=list)

    def check_budget(self, epsilon: float, delta: float = 0.0):
        if self.max_epsilon and (self.total_epsilon + epsilon > self.max_epsilon):
            raise BudgetExhaustedError(f"Epsilon budget exceeded: {self.total_epsilon + epsilon:.2f} > {self.max_epsilon}")
        if self.max_delta and (self.total_delta + delta > self.max_delta):
            raise BudgetExhaustedError(f"Delta budget exceeded: {self.total_delta + delta:.2e} > {self.max_delta}")
        
        if self.composition == "advanced":
            epsilon_used = math.sqrt(2 * self.total_epsilon * epsilon)
            delta_used = self.total_delta + delta
        else:
            epsilon_used = self.total_epsilon + epsilon
            delta_used = self.total_delta + delta

        return epsilon_used, delta_used

    def update_budget(self, epsilon: float, delta: float = 0.0):
        self.total_epsilon += epsilon
        self.total_delta += delta
        self.budget_history.append((epsilon, delta))

def validate_privacy_parameters(func):
    @wraps(func)
    def wrapper(data: Any, request: PrivacyRequest, accountant: Optional[PrivacyAccountant] = None, *args, **kwargs):
        try:
            request = request if isinstance(request, PrivacyRequest) else PrivacyRequest(**request)
        except ValidationError as e:
            raise InvalidParametersError(f"Invalid privacy parameters: {str(e)}") from e

        if accountant:
            try:
                accountant.check_budget(request.epsilon, request.delta)
            except BudgetExhaustedError as e:
                logging.error(f"Privacy budget exhausted: {str(e)}")
                raise

        return func(data, request, accountant, *args, **kwargs)
    return wrapper

@validate_privacy_parameters
def laplace_mechanism(
    data: Union[float, List[float], np.ndarray], 
    request: PrivacyRequest,
    accountant: Optional[PrivacyAccountant] = None,
    seed: Optional[int] = None
) -> Union[float, np.ndarray]:
    """Laplace mechanism for differential privacy"""
    rng = np.random.default_rng(seed)
    scale = request.sensitivity / request.epsilon
    
    if isinstance(data, (list, np.ndarray)):
        noise = rng.laplace(0, scale, len(data))
        result = np.array(data) + noise
    else:
        noise = rng.laplace(0, scale)
        result = data + noise
    
    if request.min_value is not None or request.max_value is not None:
        result = np.clip(result, request.min_value, request.max_value)
    
    if accountant:
        accountant.update_budget(request.epsilon)
    
    DP_EPSILON_USAGE.inc(request.epsilon)
    DP_QUERY_COUNT.inc()
    DP_NOISE_AMOUNT.observe(abs(noise.mean()))
    
    return result.item() if isinstance(data, (int, float)) else result

@validate_privacy_parameters
def gaussian_mechanism(
    data: Union[float, List[float], np.ndarray],
    request: PrivacyRequest,
    accountant: Optional[PrivacyAccountant] = None,
    seed: Optional[int] = None
) -> Union[float, np.ndarray]:
    """Gaussian mechanism for (ε,δ)-differential privacy"""
    rng = np.random.default_rng(seed)
    sigma = math.sqrt(2 * math.log(1.25/request.delta)) * request.sensitivity / request.epsilon
    
    if isinstance(data, (list, np.ndarray)):
        noise = rng.normal(0, sigma, len(data))
        result = np.array(data) + noise
    else:
        noise = rng.normal(0, sigma)
        result = data + noise
    
    if request.min_value is not None or request.max_value is not None:
        result = np.clip(result, request.min_value, request.max_value)
    
    if accountant:
        accountant.update_budget(request.epsilon, request.delta)
    
    DP_EPSILON_USAGE.inc(request.epsilon)
    DP_DELTA_USAGE.inc(request.delta)
    DP_QUERY_COUNT.inc()
    DP_NOISE_AMOUNT.observe(abs(noise.mean()))
    
    return result.item() if isinstance(data, (int, float)) else result

@validate_privacy_parameters
def exponential_mechanism(
    candidates: List[Any],
    request: PrivacyRequest,
    accountant: Optional[PrivacyAccountant] = None,
    seed: Optional[int] = None
) -> Any:
    """Exponential mechanism for differential privacy"""
    if not request.custom_score_fn:
        raise InvalidParametersError("Exponential mechanism requires a custom score function")
    
    rng = np.random.default_rng(seed)
    scores = np.array([request.custom_score_fn(c) for c in candidates])
    sensitivity = request.sensitivity
    
    # Normalize scores and calculate probabilities
    probabilities = np.exp(request.epsilon * scores / (2 * sensitivity))
    probabilities /= probabilities.sum()
    
    # Select candidate based on probability distribution
    selected_index = rng.choice(len(candidates), p=probabilities)
    
    if accountant:
        accountant.update_budget(request.epsilon)
    
    DP_EPSILON_USAGE.inc(request.epsilon)
    DP_QUERY_COUNT.inc()
    
    return candidates[selected_index]

@validate_privacy_parameters
def binary_random_response(
    data: bool,
    request: PrivacyRequest,
    accountant: Optional[PrivacyAccountant] = None,
    seed: Optional[int] = None
) -> bool:
    """Random response mechanism for binary data"""
    rng = np.random.default_rng(seed)
    p = math.exp(request.epsilon) / (math.exp(request.epsilon) + 1)
    
    if data:
        return rng.random() <= p
    else:
        return rng.random() > (1 - p)

def calculate_sensitivity(
    dataset: np.ndarray,
    query_fn: Callable,
    neighbor_def: str = 'add_remove'
) -> float:
    """Calculate sensitivity of a query function"""
    sensitivities = []
    
    for i in range(len(dataset)):
        if neighbor_def == 'add_remove':
            neighbor = np.delete(dataset, i)
        elif neighbor_def == 'edit':
            neighbor = dataset.copy()
            neighbor[i] = 0
        else:
            raise SensitivityError("Invalid neighbor definition")
        
        original_result = query_fn(dataset)
        neighbor_result = query_fn(neighbor)
        sensitivities.append(abs(original_result - neighbor_result))
    
    return max(sensitivities)

def moments_accountant(
    epsilons: List[float],
    deltas: List[float],
    target_delta: float
) -> float:
    """Advanced composition theorem using moments accountant"""
    from scipy.optimize import minimize_scalar
    
    def delta_mu(mu):
        return sum([math.exp(eps * mu + (mu**2 * eps**2)/8) - 1 for eps in epsilons]) + sum(deltas)
    
    res = minimize_scalar(lambda mu: abs(delta_mu(mu) - target_delta))
    return res.x

# Example Usage
if __name__ == "__main__":
    # Initialize privacy accountant with budget
    accountant = PrivacyAccountant(max_epsilon=1.0, max_delta=1e-5)
    
    # Sample dataset
    data = [1.0, 2.0, 3.0, 4.0]
    
    # Configure privacy request
    request = PrivacyRequest(
        mechanism=DPMechanism.LAPLACE,
        epsilon=0.5,
        sensitivity=1.0,
        min_value=0.0,
        max_value=5.0
    )
    
    # Apply differentially private mechanism
    private_data = laplace_mechanism(data, request, accountant)
    print(f"Private results: {private_data}")
    
    # Remaining budget check
    print(f"Remaining epsilon budget: {1.0 - accountant.total_epsilon}")

