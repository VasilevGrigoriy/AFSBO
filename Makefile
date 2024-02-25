CODE = afsbo dags
airflow_id = $(shell id -u)

format:
	black $(CODE)
	isort $(CODE)

airflow:
	echo "\nAIRFLOW_UID=$(airflow_id)" >> .env
	sudo docker compose up airflow-init
	sudo docker compose up