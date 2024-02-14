CODE = afsbo dags

format:
	black $(CODE)
	isort $(CODE)