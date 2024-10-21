current_branch = 1.0.0

build_notebook:
	@docker build -t breweries-notebook:$(current_branch) ./docker/customized/notebook

build_spark:
	@docker build -t breweries-spark:$(current_branch) ./docker/customized/spark

build_python_apps_image:
	@docker build -t breweries-python-apps:$(current_branch) ./docker/app_layer/python_jobs

build_spark_apps_image:
	@docker build -t breweries-spark-apps:$(current_branch) ./docker/app_layer/spark_jobs

deploy_services:
	@docker compose -f services/lakehouse_layer.yml up -d --build
	@docker compose -f services/processing_layer.yml up -d --build
	@docker compose -f services/orchestration_layer.yml up -d --build
	@docker compose -f services/ingestion_layer.yml up -d --build
	# @docker compose -f services/monitoring_layer.yml up -d --build

stop_services:
	@docker compose -f services/lakehouse_layer.yml down
	@docker compose -f services/processing_layer.yml down
	@docker compose -f services/orchestration_layer.yml down
	@docker compose -f services/ingestion_layer.yml down
	# @docker compose -f services/monitoring_layer.yml down

watch_services:
	@watch docker compose -f services/lakehouse_layer.yml ps