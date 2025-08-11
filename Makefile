run-no-spark:
	docker compose up --build worker-krsapi worker-krsdf -d

run-spark:
	docker compose up --build spark-etl
run-spark-d:
	docker compose up --build spark-etl -d

down:
	docker compose down -v

