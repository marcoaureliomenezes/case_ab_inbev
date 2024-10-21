import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator


COMMON_DOCKER_OP = dict(
  image="breweries-python-apps:1.0.0",
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
  f"dag_1_crawler_to_bronze",
  start_date=datetime(year=2024,month=7,day=20,hour=2),
  schedule_interval="@hourly",
  default_args=default_args,
  max_active_runs=2,
  catchup=False
  ) as dag:

    starting_process = BashOperator(
      task_id="starting_task",
      bash_command="""sleep 2"""
    )

    capture_breweries_data = DockerOperator(
      **COMMON_DOCKER_OP,
      network_mode="ab_inbev_network",
      task_id="capture_breweries_data",
      entrypoint="python /app/job_1.py",
      environment= {
      "S3_ENDPOINT": os.getenv("S3_ENDPOINT"),
      "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
      "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
      "BUCKET": "breweries",
      "END_DATE": "{{ execution_date }}"                      
      }
    )
    
    end_process = BashOperator(
      task_id="end_process",
      bash_command="""sleep 2"""
    )



    starting_process >> capture_breweries_data >> end_process