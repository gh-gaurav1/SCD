from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'gaurav',
    'start_date': days_ago(1),
}

# Define the DAG
with DAG(
    'run_pyspark_in_existing_container',
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or set to a specific schedule
) as dag:

    # Task to run main.py inside the already running PySpark container using `docker exec`
    run_spark_task = BashOperator(
        task_id='run_pyspark_main_task',
        bash_command='docker exec pyspark_scd /opt/bitnami/spark/bin/spark-submit ',
        # Replace 'pyspark_scd' with the container name or ID of the running PySpark container1
    )
