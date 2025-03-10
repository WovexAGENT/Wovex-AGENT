import asyncio
import logging
import json
import os
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List, AsyncGenerator
from aiohttp import ClientSession, TCPConnector
import asyncpg
from sklearn.externals import joblib
import numpy as np
from prometheus_client import Counter, Histogram

# Metrics Configuration
PREDICTION_COUNTER = Counter('opportunity_predictions_total', 'Total predictions made', ['status'])
API_REQUEST_TIME = Histogram('api_request_duration_seconds', 'API request latency', ['endpoint'])

@dataclass
class Opportunity:
    opportunity_id: str
    account_id: str
    amount: float
    stage: str
    probability: float
    last_updated: datetime = field(default_factory=datetime.utcnow)

class DatabaseManager:
    """Async PostgreSQL connection pool manager"""
    
    def __init__(self, dsn: str, max_connections: int = 10):
        self.pool = None
        self.dsn = dsn
        self.max_connections = max_connections
        self.logger = logging.getLogger('DBManager')

    async def connect(self):
        """Initialize connection pool"""
        self.pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=1,
            max_size=self.max_connections,
            ssl='require'
        )
        self.logger.info("Database pool initialized")

    async def fetch_opportunities(self, batch_size: int = 100) -> AsyncGenerator[Opportunity, None]:
        """Stream opportunities from database"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                query = "SELECT * FROM opportunities WHERE stage NOT IN ('closed_won', 'closed_lost')"
                async for record in conn.cursor(query):
                    yield Opportunity(
                        opportunity_id=record['opportunity_id'],
                        account_id=record['account_id'],
                        amount=float(record['amount']),
                        stage=record['stage'],
                        probability=float(record['probability']),
                        last_updated=record['last_updated']
                    )

    async def update_opportunity(self, opportunity: Opportunity):
        """Update opportunity record"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE opportunities 
                SET stage = \$1, probability = \$2, last_updated = \$3
                WHERE opportunity_id = \$4
            ''', opportunity.stage, opportunity.probability, 
               datetime.utcnow(), opportunity.opportunity_id)

class CRMAPIClient:
    """Enterprise CRM system client"""
    
    def __init__(self, base_url: str, auth_token: str):
        self.base_url = base_url
        self.auth_token = auth_token
        self.session = None
        self.logger = logging.getLogger('CRMClient')

    async def __aenter__(self):
        self.session = ClientSession(
            connector=TCPConnector(ssl=False),
            headers={'Authorization': f'Bearer {self.auth_token}'}
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()

    @API_REQUEST_TIME.time()
    async def sync_opportunity(self, opportunity: Opportunity):
        """Sync opportunity state with CRM"""
        endpoint = f'{self.base_url}/opportunities/{opportunity.opportunity_id}'
        payload = {
            'stage': opportunity.stage,
            'probability': opportunity.probability,
            'last_updated': opportunity.last_updated.isoformat()
        }
        
        try:
            async with self.session.patch(endpoint, json=payload) as response:
                if response.status == 429:
                    await asyncio.sleep(1)
                    return await self.sync_opportunity(opportunity)
                    
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            self.logger.error(f"CRM sync failed: {str(e)}")
            raise

class OpportunityAgent:
    """Core opportunity prediction and management engine"""
    
    def __init__(self):
        self.model = None
        self.db = DatabaseManager(os.getenv('DB_DSN'))
        self.crm = CRMAPIClient(
            os.getenv('CRM_API_URL'),
            os.getenv('CRM_AUTH_TOKEN')
        )
        self.logger = logging.getLogger('OpportunityAgent')
        self.prediction_cache = {}

    async def initialize(self):
        """Load resources and connections"""
        await self.db.connect()
        self._load_model()
        self.logger.info("Agent initialization complete")

    def _load_model(self):
        """Load ML model from persistent storage"""
        model_path = os.getenv('MODEL_PATH', 'model.pkl')
        try:
            self.model = joblib.load(model_path)
            self.logger.info(f"Loaded model from {model_path}")
        except Exception as e:
            self.logger.critical(f"Model load failed: {str(e)}")
            raise

    async def predict_opportunity_outcome(self, opportunity: Opportunity) -> Dict[str, Any]:
        """Predict opportunity stage and probability"""
        if opportunity.opportunity_id in self.prediction_cache:
            return self.prediction_cache[opportunity.opportunity_id]

        features = self._extract_features(opportunity)
        prediction = self.model.predict_proba([features])[0]
        
        result = {
            'stage': self.model.predict([features])[0],
            'probability': float(prediction[1]),  # Assuming binary classification
            'features': features
        }
        
        self.prediction_cache[opportunity.opportunity_id] = result
        PREDICTION_COUNTER.labels(status='success').inc()
        return result

    def _extract_features(self, opportunity: Opportunity) -> List[float]:
        """Convert business data to model features"""
        return [
            opportunity.amount,
            self._stage_to_numeric(opportunity.stage),
            (datetime.utcnow() - opportunity.last_updated).days
        ]

    @staticmethod
    def _stage_to_numeric(stage: str) -> int:
        """Convert sales stage to numeric representation"""
        stages = {
            'prospecting': 0,
            'qualification': 1,
            'needs_analysis': 2,
            'value_proposition': 3,
            'negotiation': 4
        }
        return stages.get(stage.lower(), -1)

    async def process_opportunities(self, interval: int = 300):
        """Main processing loop"""
        while True:
            self.logger.info("Starting opportunity processing cycle")
            
            try:
                async with self.crm as crm_client:
                    async for opportunity in self.db.fetch_opportunities():
                        prediction = await self.predict_opportunity_outcome(opportunity)
                        
                        # Update local record
                        opportunity.stage = prediction['stage']
                        opportunity.probability = prediction['probability']
                        await self.db.update_opportunity(opportunity)

                        # Sync with CRM
                        await crm_client.sync_opportunity(opportunity)
                        
            except Exception as e:
                self.logger.error(f"Processing cycle failed: {str(e)}")
                PREDICTION_COUNTER.labels(status='failure').inc()
            
            await asyncio.sleep(interval)

def configure_logging():
    """Configure structured logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('opportunity_agent.log')
        ]
    )

async def main():
    configure_logging()
    agent = OpportunityAgent()
    
    try:
        await agent.initialize()
        await agent.process_opportunities()
    except KeyboardInterrupt:
        logging.info("Shutting down gracefully")

if __name__ == "__main__":
    asyncio.run(main())
