import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator


COMMON_KWARGS_DOCKER_OPERATOR = dict(
  network_mode="ab_inbev_network",
  docker_url="unix:/var/run/docker.sock",
  auto_remove=True,
  mount_tmp_dir=False,
  tty=False,
)

default_args ={
  "owner": "airflow",
  "email_on_failure": False,
  "email_on_retry": False,
  "email": "marco_aurelio_reis@yahoo.com.br",
  "retries": 1,
  "retry_delay": timedelta(minutes=5) 
}

with DAG(
  f"dag_daily_breweries",
  start_date=datetime(year=2024,month=10,day=30),
  schedule_interval="@daily",
  default_args=default_args,
  max_active_runs=2,
  catchup=False
  ) as dag:

    starting_process = BashOperator(
      task_id="starting_task",
      bash_command="""sleep 2"""
    )


    breweries_bronze_to_silver= DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      image="breweries-spark-apps:1.0.0",
      task_id="breweries_bronze_to_silver",
      entrypoint="sh /app/1_bronze_to_silver/spark-submit.sh",
      environment= {
      "S3_ENDPOINT": os.getenv("S3_ENDPOINT"),
      "NESSIE_URI": os.getenv("NESSIE_URI"),
      "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION"),
      "AWS_REGION": os.getenv("AWS_DEFAULT_REGION"),
      "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
      "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
      "BUCKET": "breweries",
      "EXECUTION_DATE": "{{ execution_date }}"                      
      }
    )
    

    end_process = BashOperator(
      task_id="end_process",
      bash_command="""sleep 2"""
    )


    
    starting_process >> breweries_bronze_to_silver >> end_process