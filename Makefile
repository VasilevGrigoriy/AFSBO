CODE = afsbo dags
airflow_id = $(shell id -u)

format:
	black $(CODE)
	isort $(CODE)

airflow:
	echo "\nAIRFLOW_UID=$(airflow_id)" >> .env
	sudo docker compose up airflow-init
	sudo docker compose up

docker-install:
	sudo apt update
	sudo apt install curl software-properties-common ca-certificates apt-transport-https -y
	wget -O- https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor | sudo tee /etc/apt/keyrings/docker.gpg > /dev/null
	echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu jammy stable"| sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
	sudo apt update
	apt-cache policy docker-ce
	sudo apt install docker-ce -y

poetry-install:
	curl -sSL https://install.python-poetry.org | python3 -
	export PATH=$(HOME)/.local/bin:$(PATH)