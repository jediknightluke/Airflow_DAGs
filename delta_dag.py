import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession

# Initialize Spark with Delta Lake support
spark = SparkSession.builder \
    .appName("DeltaLakeWithAirflow") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:x.y.z") \
    .getOrCreate()

default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'email': ['me@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'delta_lake_etl',
    default_args=default_args,
    description='An ETL task using Delta Lake',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
)

def extract(*args, **kwargs):
    # Assuming you have data in 'source_data.csv'
    df = spark.read.csv('source_data.csv', header=True)
    df.write.format("delta").mode("overwrite").save("/tmp/raw_data.delta")

def transform(*args, **kwargs):
    delta_df = spark.read.format("delta").load("/tmp/raw_data.delta")
    
    # Basic transformation example
    transformed_df = delta_df.withColumnRenamed("old_column", "new_column")
    
    transformed_df.write.format("delta").mode("overwrite").save("/tmp/transformed_data.delta")

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    dag=dag,
)

extract_task >> transform_task
