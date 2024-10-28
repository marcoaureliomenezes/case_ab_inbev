current_branch = 1.0.0

build_notebook:
	@docker build -t breweries-notebook:$(current_branch) ./docker/customized/notebook

build_spark:
	@docker build -t breweries-spark:$(current_branch) ./docker/customized/spark

build_python_apps_image:
	@docker build -t breweries-python-apps:$(current_branch) ./docker/app/python_jobs

build_spark_apps_image:
	@docker build -t breweries-spark-apps:$(current_branch) ./docker/app/spark_jobs

deploy_services:
	@docker compose -f services/lakehouse.yml up -d --build
	@docker compose -f services/processing.yml up -d --build
	@docker compose -f services/orchestration.yml up -d --build
	@docker compose -f services/ingestion.yml up -d --build
	# @docker compose -f services/monitoring.yml up -d --build

stop_services:
	@docker compose -f services/lakehouse.yml down
	@docker compose -f services/processing.yml down
	@docker compose -f services/orchestration.yml down
	@docker compose -f services/ingestion.yml down
	# @docker compose -f services/monitoring.yml down

watch_services:
	@watch docker compose -f services/lakehouse.yml ps