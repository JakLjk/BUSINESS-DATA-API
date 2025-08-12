run-base:
	docker compose up --build worker-krsapi worker-krsdf -d

run-spark:
	docker compose up --build spark-etl

run-spark-d:
	docker compose up --build spark-etl -d

run-automation-krsapi:
	docker compose up --build automation-krsapi-refresh

run-automation-krsapi-d:
	docker compose up --build automation-krsapi-refresh -d

down:
	docker compose down -v

down-spark:
	docker compose stop automation-krsapi-refresh
	docker compose rm -f automation-krsapi-refresh
down-automation-krsapi:
	docker compose stop automation-krsapi-refresh
	docker compose rm -f automation-krsapi-refresh

