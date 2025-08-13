run-base-d:
	docker compose up --build worker-krsapi worker-krsdf -d

run-base-scaled-krsdf-d:
	docker compose up --build --scale worker-krsdf=2 worker-krsapi worker-krsdf -d

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
	docker compose stop spark-etl
	docker compose rm -f spark-etl
down-automation-krsapi:
	docker compose stop automation-krsapi-refresh
	docker compose rm -f automation-krsapi-refresh

