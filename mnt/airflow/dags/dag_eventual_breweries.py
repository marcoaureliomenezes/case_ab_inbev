import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator


COMMON_KWARGS_DOCKER_OPERATOR = dict(
  image="breweries-spark-apps:1.0.0",
  network_mode="ab_inbev_network",
  docker_url="unix:/var/run/docker.sock",
  auto_remove=True,
  mount_tmp_dir=False,
  tty=False,
)

COMMON_SPARK_VARS = dict(
  S3_ENDPOINT = os.getenv("S3_ENDPOINT"),
  NESSIE_URI = os.getenv("NESSIE_URI"),
  AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID"),
  AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY"),
  AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION"),
  AWS_REGION = os.getenv("AWS_REGION"),
  
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
  f"dag_eventual_breweries",
  start_date=datetime(year=2024,month=7,day=20,hour=2),
  schedule_interval="@once",
  default_args=default_args,
  max_active_runs=2,
  catchup=False
  ) as dag:

    starting_process = BashOperator(
      task_id="starting_task",
      bash_command="""sleep 2"""
    )


    create_namespace_silver = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      task_id="create_namespace_silver",
      entrypoint="sh /app/0_breweries_ddls/eventual_jobs/submit-ddl.sh 1_namespace_silver/pyspark_app.py",
      environment= dict(
          **COMMON_SPARK_VARS,
          NAMESPACE_NAME = "nessie.silver")
    )

    create_table_silver_breweries = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      task_id="create_table_silver_breweries",
      entrypoint="sh /app/0_breweries_ddls/eventual_jobs/submit-ddl.sh 3_table_breweries_silver/pyspark_app.py",
      environment= dict(
          **COMMON_SPARK_VARS,
          TABLE_NAME = "nessie.silver.breweries")
    )


    create_namespace_gold = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      task_id="create_namespace_gold",
      entrypoint="sh /app/0_breweries_ddls/eventual_jobs/submit-ddl.sh 2_namespace_gold/pyspark_app.py",
      environment= dict(
          **COMMON_SPARK_VARS,
          NAMESPACE_NAME = "nessie.gold"
      )
    )


    create_view_gold_breweries = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      task_id="create_view_gold_breweries",
      entrypoint="sh /app/0_breweries_ddls/eventual_jobs/submit-ddl.sh 4_view_breweries_gold/pyspark_app.py",
      environment= dict(
          **COMMON_SPARK_VARS,
          TABLE_NAME = "nessie.gold.breweries"
      )
    )



    starting_process >> create_namespace_silver >> create_table_silver_breweries
    starting_process >> create_namespace_gold >> create_view_gold_breweries