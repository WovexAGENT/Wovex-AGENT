from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import logging
import json
import pandas as pd
import numpy as np
import requests

# Configuration
FINANCE_CONFIG = Variable.get("financial_settings", deserialize_json=True)
SLACK_CONN_ID = "slack_finance_alerts"
ERP_API_CONN_ID = "erp_system_api"
S3_CONN_ID = "financial_data_lake"

default_args = {
    "owner": "financial_team",
    "depends_on_past": True,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "max_active_runs": 1,
    "execution_timeout": timedelta(hours=4)
}

logger = logging.getLogger("airflow.task")

def validate_financial_files(**context):
    execution_date = context["execution_date"].strftime("%Y-%m-%d")
    
    try:
        # Check file existence in SFTP
        sftp_files = SFTPOperator(
            task_id="tmp_list_files",
            sftp_conn_id="sftp_financial",
            command=f"ls /incoming/{execution_date}",
            do_xcom_push=True
        ).execute(context)
        
        required_files = {"transactions.csv", "ledger.csv", "forex_rates.csv"}
        missing_files = required_files - set(sftp_files.split("\n"))
        
        if missing_files:
            error_msg = f"Missing required files: {', '.join(missing_files)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        context["ti"].xcom_push(key="validated_files", value=True)
        
    except Exception as e:
        logger.error(f"File validation failed: {str(e)}")
        raise

def transform_fx_rates(**context):
    execution_date = context["execution_date"]
    
    try:
        # SFTP file retrieval
        sftp_get = SFTPOperator(
            task_id="tmp_get_fx_file",
            sftp_conn_id="sftp_financial",
            local_filepath="/tmp/fx_rates.csv",
            remote_filepath=f"/incoming/{execution_date}/forex_rates.csv",
            operation="get",
            create_intermediate_dirs=True
        ).execute(context)
        
        # Data processing
        fx_df = pd.read_csv("/tmp/fx_rates.csv", parse_dates=["date"])
        fx_df = fx_df[f"date == '{execution_date}'"]
        
        # Add cross rates
        fx_df["EUR/USD"] = fx_df["USD/EUR"].apply(lambda x: 1/x)
        fx_df["GBP/USD"] = fx_df["USD/GBP"].apply(lambda x: 1/x)
        
        # Save processed data
        processed_path = f"s3://{FINANCE_CONFIG['data_lake']}/forex/{execution_date}.parquet"
        fx_df.to_parquet(processed_path)
        
        context["ti"].xcom_push(key="fx_processed", value=True)
        
    except Exception as e:
        logger.error(f"FX processing failed: {str(e)}")
        raise

def reconcile_transactions(**context):
    try:
        # Get data from multiple sources
        erp_data = requests.get(
            f"{FINANCE_CONFIG['erp_api']}/transactions",
            params={"date": context["execution_date"]},
            headers={"Authorization": f"Bearer {FINANCE_CONFIG['api_key']}"}
        ).json()
        
        bank_data = requests.get(
            f"{FINANCE_CONFIG['bank_api']}/statements",
            params={"date": context["execution_date"]},
            headers={"Authorization": f"Bearer {FINANCE_CONFIG['api_key']}"}
        ).json()
        
        # Reconciliation logic
        discrepancies = []
        for erp_tx in erp_data:
            bank_match = next(
                (b for b in bank_data if b["reference"] == erp_tx["tx_id"]), 
                None
            )
            
            if not bank_match:
                discrepancies.append({
                    "type": "missing_bank_entry",
                    "tx_id": erp_tx["tx_id"],
                    "amount": erp_tx["amount"]
                })
            elif abs(bank_match["amount"] - erp_tx["amount"]) > 0.01:
                discrepancies.append({
                    "type": "amount_mismatch",
                    "tx_id": erp_tx["tx_id"],
                    "erp_amount": erp_tx["amount"],
                    "bank_amount
