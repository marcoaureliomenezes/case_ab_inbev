import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator


COMMON_KWARGS_DOCKER_OPERATOR = dict(
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

    breweries_capture_and_ingest = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      image="breweries-python-apps:1.0.0",
      network_mode="ab_inbev_network",
      task_id="breweries_capture_and_ingest",
      entrypoint="python /app/1_crawler_api_breweries.py",
      environment= {
      "S3_ENDPOINT": os.getenv("S3_ENDPOINT"),
      "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
      "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
      "BUCKET": "breweries",
      "END_DATE": "{{ execution_date }}"                      
      }
    )
    
    check_breweries_data_length = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      image="breweries-python-apps:1.0.0",
      network_mode="ab_inbev_network",
      task_id="check_breweries_data_length",
      entrypoint="python /app/2_check_breweries_size.py",
      environment= {
      "S3_ENDPOINT": os.getenv("S3_ENDPOINT"),
      "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
      "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
      "BUCKET": "breweries",
      "END_DATE": "{{ execution_date }}"                      
      }
    )

    notify_incomplete_data = BashOperator(
      task_id="notify_incomplete_data",
      bash_command="""sleep 2"""
    )

    breweries_raw_to_bronze = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      image="breweries-spark-apps:1.0.0",
      network_mode="ab_inbev_network",
      task_id="breweries_raw_to_bronze",
      entrypoint="python /app/crawler_api_breweries.py",
      environment= {
      "S3_ENDPOINT": os.getenv("S3_ENDPOINT"),
      "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
      "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
      "BUCKET": "breweries",
      "END_DATE": "{{ execution_date }}"                      
      }
    )

    breweries_bronze_to_silver= DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      image="breweries-spark-apps:1.0.0",
      task_id="breweries_bronze_to_silver",
      entrypoint="python /app/crawler_api_breweries.py",
      environment= {
      "S3_ENDPOINT": os.getenv("S3_ENDPOINT"),
      "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
      "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
      "BUCKET": "breweries",
      "END_DATE": "{{ execution_date }}"                      
      }
    )
    
    breweries_silver_to_gold= DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      image="breweries-spark-apps:1.0.0",
      network_mode="ab_inbev_network",
      task_id="breweries_silver_to_gold",
      entrypoint="python /app/crawler_api_breweries.py",
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


    starting_process >> breweries_capture_and_ingest >> check_breweries_data_length >> notify_incomplete_data
    
    starting_process >> breweries_capture_and_ingest >> breweries_raw_to_bronze >> breweries_bronze_to_silver >> breweries_silver_to_gold >> end_process