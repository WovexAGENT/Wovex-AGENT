import os
import csv
import time
import random
import json
from datetime import datetime
from locust import HttpUser, task, between, events, LoadTestShape
from locust.runners import MasterRunner, WorkerRunner
from locust.env import Environment
from locust_plugins.csvreader import CSVReader

# ======================
# Configuration Settings
# ======================
HOST = os.getenv("LOAD_TEST_HOST", "https://api.example.com")
API_KEY = os.getenv("API_KEY", "your-api-key")
TEST_DATA_CSV = "test_data.csv"
MAX_USERS = int(os.getenv("MAX_USERS", "1000"))
SPAWN_RATE = int(os.getenv("SPAWN_RATE", "100"))

# ====================
# Custom CSV Data Set
# ====================
class DatasetReader:
    def __init__(self):
        self.accounts = CSVReader(TEST_DATA_CSV)
    
    def get_random_account(self):
        return next(self.accounts)

dataset = DatasetReader()

# ==================
# Custom Statistics
# ==================
@events.init.add_listener
def init_stats(environment: Environment, **kwargs):
    environment.stats.custom_stats.register("business_transactions", "Business Transactions Completed")
    environment.stats.custom_stats.register("failed_logins", "Authentication Failures")

# ===================
# Authentication Flow
# ===================
class AuthClient:
    def __init__(self, client):
        self.client = client
        self.token = None
    
    def login(self):
        payload = {
            "username": os.getenv("TEST_USER", "load_user"),
            "password": os.getenv("TEST_PASS", "load_password")
        }
        response = self.client.post("/auth/login", json=payload)
        if response.status_code == 200:
            self.token = response.json()["access_token"]
            return True
        return False

# =====================
# Base Load Test Class
# =====================
class ApiUser(HttpUser):
    host = HOST
    wait_time = between(1, 5)
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.auth = AuthClient(self.client)
        self.headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
    
    def on_start(self):
        if not self.auth.login():
            self.environment.runner.quit()
            raise Exception("Failed to authenticate load test user")
        
        self.headers["Authorization"] = f"Bearer {self.auth.token}"
        self.client.verify = False  # Disable SSL verification for testing

    # ==============
    # Test Scenarios
    # ==============
    @task(5)
    def get_user_profile(self):
        account = dataset.get_random_account()
        with self.client.get(f"/users/{account['id']}", 
                            headers=self.headers,
                            name="/users/[id]",
                            catch_response=True) as response:
            if response.status_code == 404:
                response.failure("User not found")
                self.environment.stats.custom_stats.increment("failed_logins")
            else:
                response.success()
                self.environment.stats.custom_stats.increment("business_transactions")

    @task(3)
    def update_account(self):
        account = dataset.get_random_account()
        payload = {
            "email": f"updated_{int(time.time())}@test.com",
            "metadata": {
                "loadTestRunId": self.environment.runner.run_id
            }
        }
        self.client.put(f"/accounts/{account['id']}", 
                       json=payload,
                       headers=self.headers,
                       name="/accounts/[id]")

    @task(2)
    def search_operations(self):
        params = {
            "query": random.choice(["active", "pending", "expired"]),
            "page": random.randint(1, 100)
        }
        self.client.get("/search", 
                       params=params,
                       headers=self.headers,
                       name="/search")

    @task(1)
    def critical_transaction(self):
        with self.client.post("/transactions", 
                            json={
                                "amount": random.uniform(10, 1000),
                                "currency": "USD"
                            },
                            headers=self.headers,
                            name="/transactions") as response:
            if response.status_code == 201:
                self.environment.stats.custom_stats.increment("business_transactions")

# ====================
# Load Test Scenarios
# ====================
class StepLoadShape(LoadTestShape):
    stages = [
        {"duration": 300, "users": 100, "spawn_rate": 10},
        {"duration": 600, "users": 500, "spawn_rate": 50},
        {"duration": 900, "users": 1000, "spawn_rate": 100},
        {"duration": 1200, "users": 500, "spawn_rate": 10},
        {"duration": 1500, "users": 100, "spawn_rate": 10}
    ]
    
    def tick(self):
        run_time = self.get_run_time()
        for stage in self.stages:
            if run_time < stage["duration"]:
                return (stage["users"], stage["spawn_rate"])
        return None

# ==================
# Event Hooks
# ==================
@events.test_start.add_listener
def on_test_start(environment: Environment, **kwargs):
    print(f"Load test initiated at {datetime.now().isoformat()}")
    if isinstance(environment.runner, WorkerRunner):
        print(f"Worker {environment.runner.worker_index} connected")

@events.test_stop.add_listener
def on_test_stop(environment: Environment, **kwargs):
    print(f"Load test completed at {datetime.now().isoformat()}")
    environment.stats.custom_stats.dump()

# ================
# Custom Commands
# ================
if __name__ == "__main__":
    from locust.main import main
    main()
