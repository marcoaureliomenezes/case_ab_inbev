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
  f"dag_hourly_breweries",
  start_date=datetime(year=2024,month=10,day=29,hour=0),
  schedule_interval="@hourly",
  default_args=default_args,
  max_active_runs=2,
  catchup=True
  ) as dag:

    starting_process = BashOperator(
      task_id="starting_task",
      bash_command="""sleep 2"""
    )

    breweries_capture_and_ingest = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      image="breweries-python-apps:1.0.0",
      task_id="breweries_capture_and_ingest",
      entrypoint="python /app/1_crawler_breweries.py",
      environment= {
      "S3_ENDPOINT": os.getenv("S3_ENDPOINT"),
      "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
      "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
      "BUCKET": "breweries",
      "EXECUTION_DATE": "{{ execution_date }}"                      
      }
    )
    
    check_breweries_data_length = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      image="breweries-python-apps:1.0.0",
      task_id="check_breweries_data_length",
      entrypoint="python /app/2_checker_breweries.py",
      environment= {
      "S3_ENDPOINT": os.getenv("S3_ENDPOINT"),
      "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
      "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
      "BUCKET": "breweries",
      "EXECUTION_DATE": "{{ execution_date }}"                      
      }
    )

    notify_incomplete_data = BashOperator(
      task_id="notify_incomplete_data",
      bash_command="""sleep 2"""
    )

    end_process = BashOperator(
      task_id="end_process",
      bash_command="""sleep 2"""
    )


    starting_process >> breweries_capture_and_ingest >> check_breweries_data_length >> [notify_incomplete_data , end_process]