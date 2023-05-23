from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.utils.dates import days_ago

BASE_PATH = '/opt/airflow/dags/spark'
PACKAGES = 'io.openlineage:openlineage-spark:0.3.0,org.apache.hadoop:hadoop-aws:3.2.0'
ENV_VARS = {
    'AWS_ACCESS_KEY': 'minio',
    'AWS_SECRET_KEY': '12345678'
}

default_args = {
    'owner': 'data eng',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['john@doe.com']
}

dag = DAG(
    'ETL',
    schedule_interval='0 0 * * *',
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args=default_args,
    description='Spark ETL. Raw -> Transformed -> Analytics.'
)

Raw = SparkSubmitOperator(
    application = f"{BASE_PATH}/raw_etl.py",
    conn_id= 'spark_local', 
    packages=PACKAGES,
    task_id='spark_raw_etl_task', 
    env_vars=ENV_VARS,
    dag=dag
)

Transformed = SparkSubmitOperator(
    application = f"{BASE_PATH}/transformed_etl.py",
    conn_id= 'spark_local', 
    packages=PACKAGES,
    task_id='spark_transformed_etl_task', 
    env_vars=ENV_VARS,
    dag=dag
)
    
Analytics = SparkSubmitOperator(
    application = f"{BASE_PATH}/analytics_etl.py",
    conn_id= 'spark_local', 
    packages=PACKAGES,
    task_id='spark_analytics_etl_task', 
    env_vars=ENV_VARS,
    dag=dag
)

Raw.set_downstream(Transformed)
Transformed.set_downstream(Analytics)
