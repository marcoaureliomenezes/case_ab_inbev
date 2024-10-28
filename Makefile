current_branch = 1.0.0

build_images:
	@docker build -t breweries-spark:$(current_branch) 				./docker/customized/spark
	@docker build -t breweries-spark-notebooks:$(current_branch) 	./docker/customized/notebook
	@docker build -t breweries-spark-apps:$(current_branch) 		./docker/app/spark_jobs
	@docker build -t breweries-python-apps:$(current_branch) 		./docker/app/python_jobs


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