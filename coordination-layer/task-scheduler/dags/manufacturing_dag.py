from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import logging
import json
import requests

# Configuration
MANUFACTURING_CONFIG = Variable.get("manufacturing_settings", deserialize_json=True)
SLACK_CONN_ID = "slack_alerts"
ERP_API_CONN_ID = "erp_system"
MES_API_CONN_ID = "mes_system"

default_args = {
    "owner": "manufacturing_team",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "execution_timeout": timedelta(hours=2)
}

logger = logging.getLogger("airflow.task")

def validate_production_order(**context):
    order_data = context["params"].get("order_details")
    
    required_fields = ["product_id", "quantity", "due_date", "priority"]
    if not all(field in order_data for field in required_fields):
        error_msg = f"Invalid order format. Missing required fields: {required_fields}"
        logger.error(error_msg)
        raise ValueError(error_msg)
        
    if order_data["quantity"] <= 0:
        error_msg = f"Invalid quantity: {order_data['quantity']}"
        logger.error(error_msg)
        raise ValueError(error_msg)
        
    context["ti"].xcom_push(key="validated_order", value=order_data)
    logger.info(f"Validated order: {order_data['order_id']}")

def check_material_availability(**context):
    order_data = context["ti"].xcom_pull(key="validated_order")
    
    query = f"""
        SELECT material_id, required_qty 
        FROM bom_materials 
        WHERE product_id = '{order_data["product_id"]}'
    """
    
    try:
        result = context["ti"].xcom_pull(
            task_ids="get_bom_materials",
            key="return_value"
        )
        
        insufficient_materials = []
        for material in result:
            inventory = requests.get(
                f"{MANUFACTURING_CONFIG['inventory_api']}/stock/{material['material_id']}",
                headers={"Authorization": f"Bearer {MANUFACTURING_CONFIG['api_key']}"}
            ).json()
            
            if inventory["available"] < material["required_qty"] * order_data["quantity"]:
                insufficient_materials.append(material["material_id"])
                
        if insufficient_materials:
            error_msg = f"Insufficient materials: {', '.join(insufficient_materials)}"
            logger.error(error_msg)
            context["ti"].xcom_push(key="material_shortage", value=insufficient_materials)
            return "handle_material_shortage"
            
        return "schedule_production"
        
    except Exception as e:
        logger.error(f"Material check failed: {str(e)}")
        raise

def generate_production_schedule(**context):
    order_data = context["ti"].xcom_pull(key="validated_order")
    
    schedule_payload = {
        "product_id": order_data["product_id"],
        "quantity": order_data["quantity"],
        "priority": order_data["priority"],
        "work_centers": MANUFACTURING_CONFIG["work_centers"],
        "due_date": order_data["due_date"]
    }
    
    try:
        response = requests.post(
            MANUFACTURING_CONFIG["scheduler_api"],
            json=schedule_payload,
            timeout=30
        )
        response.raise_for_status()
        
        schedule = response.json()
        context["ti"].xcom_push(key="production_schedule", value=schedule)
        logger.info(f"Generated schedule for {order_data['product_id']}")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Scheduling API error: {str(e)}")
        raise

def execute_work_order(**context):
    schedule = context["ti"].xcom_pull(key="production_schedule")
    
    for operation in schedule["operations"]:
        logger.info(f"Starting operation {operation['op_id']}")
        
        try:
            mes_response = requests.post(
                f"{MANUFACTURING_CONFIG['mes_api']}/workorders",
                json=operation,
                headers={"Content-Type": "application/json"}
            )
            mes_response.raise_for_status()
            
            operation_status = mes_response.json()["status"]
            context["ti"].xcom_push(key=f"op_{operation['op_id']}_status", value=operation_status)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Operation {operation['op_id']} failed: {str(e)}")
            raise

def quality_check(**context):
    order_data = context["ti"].xcom_pull(key="validated_order")
    
    try:
        qc_report = requests.get(
            f"{MANUFACTURING_CONFIG['quality_api']}/reports/{order_data['order_id']}"
        ).json()
        
        if qc_report["defect_rate"] > MANUFACTURING_CONFIG["max_defect_rate"]:
            logger.error(f"High defect rate: {qc_report['defect_rate']}%")
            return "handle_quality_issue"
            
        return "update_inventory"
        
    except Exception as e:
        logger.error(f"Quality check failed: {str(e)}")
        raise

def handle_exception(context):
    exception = context.get("exception")
    task_instance = context["task_instance"]
    
    alert_msg = {
        "text": f":x: Manufacturing Error in {task_instance.task_id}",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Task Failed*: {task_instance.task_id}\n*Error*: {str(exception)}"
                }
            }
        ]
    }
    
    SlackWebhookOperator(
        task_id="slack_alert",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=alert_msg,
        trigger_rule=TriggerRule.ONE_FAILED
    ).execute(context)
    
    # Implement rollback logic
    if "production_schedule" in task_instance.xcom_pull():
        logger.info("Rolling back production schedule")
        # Add actual rollback API calls here

with DAG(
    dag_id="advanced_manufacturing_workflow",
    default_args=default_args,
    description="End-to-end manufacturing production workflow",
    schedule_interval="@daily",
    catchup=False,
    max_active_tasks=5,
    tags=["manufacturing", "production"],
    on_failure_callback=handle_exception
) as dag:
    
    start_pipeline = DummyOperator(task_id="start_manufacturing_process")
    
    validate_order = PythonOperator(
        task_id="validate_production_order",
        python_callable=validate_production_order,
        provide_context=True
    )
    
    wait_for_erp = ExternalTaskSensor(
        task_id="wait_for_erp_sync",
        external_dag_id="erp_daily_sync",
        external_task_id="complete_erp_export",
        execution_delta=timedelta(hours=1),
        timeout=3600,
        mode="reschedule"
    )
    
    get_materials = PostgresOperator(
        task_id="get_bom_materials",
        postgres_conn_id="postgres_prod",
        sql="sql/get_bom_materials.sql",
        parameters={"product_id": "{{ ti.xcom_pull(task_ids='validate_production_order')['product_id'] }}"}
    )
    
    material_check = BranchPythonOperator(
        task_id="check_material_availability",
        python_callable=check_material_availability,
        provide_context=True
    )
    
    handle_shortage = SlackWebhookOperator(
        task_id="handle_material_shortage",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=":warning: Material shortage detected! Triggering procurement process.",
        trigger_rule=TriggerRule.ONE_SUCCESS
    )
    
    create_schedule = PythonOperator(
        task_id="schedule_production",
        python_callable=generate_production_schedule,
        provide_context=True
    )
    
    execute_production = PythonOperator(
        task_id="execute_work_orders",
        python_callable=execute_work_order,
        provide_context=True,
        execution_timeout=timedelta(hours=1)
    )
    
    quality_control = BranchPythonOperator(
        task_id="perform_quality_check",
        python_callable=quality_check,
        provide_context=True
    )
    
    handle_quality = SlackWebhookOperator(
        task_id="handle_quality_issue",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=":x: Quality issues detected! Initiating rework process."
    )
    
    update_inventory = SimpleHttpOperator(
        task_id="update_inventory",
        http_conn_id="inventory_api",
        endpoint="/update",
        method="POST",
        data=json.dumps({
            "order_id": "{{ ti.xcom_pull(task_ids='validate_production_order')['order_id'] }}",
            "items": "{{ ti.xcom_pull(task_ids='execute_work_orders') }}"
        }),
        response_check=lambda response: response.json()["status"] == "success"
    )
    
    generate_report = DummyOperator(
        task_id="generate_production_report",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    complete_pipeline = DummyOperator(task_id="manufacturing_process_complete")

    # Define workflow
    start_pipeline >> validate_order >> wait_for_erp >> get_materials >> material_check
    material_check >> [handle_shortage, create_schedule]
    create_schedule >> execute_production >> quality_control
    quality_control >> [handle_quality, update_inventory]
    update_inventory >> generate_report >> complete_pipeline
    handle_shortage >> complete_pipeline
    handle_quality >> complete_pipeline

# Optional: Add cross-DAG dependencies
if MANUFACTURING_CONFIG.get("enable_maintenance_check"):
    maintenance_check = ExternalTaskSensor(
        task_id="check_equipment_maintenance",
        external_dag_id="equipment_maintenance",
        external_task_id="complete_maintenance_check",
        execution_delta=timedelta(minutes=30),
        timeout=1800
    )
    start_pipeline >> maintenance_check >> validate_order
