# BUSINESS DATA API
API for scraping, parsing and storing information about companies registered in Poland.

## Features
- Scrape financial documents from official government site in bulk
- Fetch detailed information about business from official government API
- Store collected data in local repository
- Return requested business information throught API endpoint
- Store metadata about scraping job status in Redis
- Query scraping job status through API endpoint
- Scraping jobs are handled by workers that can scale horizontally

## Technologies used
- requests – HTTP library for making API calls (>=2.32.3,<3.0.0)
- bs4 (BeautifulSoup4) – Parsing HTML and XML documents (>=0.0.2,<0.0.3)
- lxml – High-performance XML and HTML parser (>=5.4.0,<6.0.0)
- redis – Redis client for Python (>=6.2.0,<7.0.0)
- rq – Task queue library using Redis (>=2.3.3,<3.0.0)
- dotenv – Loads environment variables from `.env` files (>=0.9.9,<0.10.0)
- colorlog – Colored log output for better readability (>=6.9.0,<7.0.0)
- sqlalchemy – SQL toolkit and Object-Relational Mapping (ORM) library (>=2.0.41,<3.0.0)
- fastapi – High-performance web framework for building APIs (>=0.115.13,<0.116.0)
- uvicorn – ASGI server for running FastAPI applications (>=0.34.3,<0.35.0)
- asyncpg – Asynchronous PostgreSQL client (>=0.30.0,<0.31.0)
- psycopg – PostgreSQL database adapter for Python (>=3.2.9,<4.0.0)
- greenlet – Lightweight concurrency primitives for Python (>=3.2.3,<4.0.0)
- pydantic[email] – Data parsing and validation with email field support (>=2.11.7,<3.0.0)

## Installation
Project requires poetry in order to install all dependecies that are listed in 'pyptoject.toml'
1. Clone git repository: 
```bash
git clone https://github.com/JakLjk/BUSINESS-DATA-API
```
2. Install dependencies
```bash
poetry install
```
3. Populate .env file (you can use template .env.example, by renaming it to .env) with urls pointing to postgresql server and redis server, which are necessary to store scraped data and to manage workers.
4. To run the server run the command:
```bash
poetry run uvicorn wsgi:app --host <host ip> --port <port>
```
5. To run workers go into:
```bash
cd ./business_data_api/workers/krsdf_worker.py
```
6. To run workers:
- responsible for scraping business documents run the command:
```bash
poetry run krsdf_worker.py
```
- responsible for scraping and transforming data from official KRS API run command:
```bash
poetry run krsapi_worker.py
```
7. To run spark stream job responsible for ETL process for raw KRS API DATA run command:
```bash
poetry run python run_spark.py
```
## How to use the tool
### To get data  for specific company you need to know it's KRS number, which is unique number assigned to business entities registered in Poland's National Court Registrer.
Documentation for KRS API and KRS DF endpoints and their corresponding functions can be accessed by opening webpage: `<server ip>:<server port>/docs`

## Additional tools
### In automation scripts folder you can find additional tools that can help with populating the database 
You can use command
```
poetry run python run_automation.py
```
or
```bash
cd /automation_scripts
poetry run check_for_krs_updates --api-url <ip to the business data api> --days <how many days to check>
```
This automation script can be used in order to scrape changes for krs numbers that were registered in official KRS API registry. 
Those changes are then send as query to the business data API in order to scrape information about current extract and financial documents.
This script can be used to i.e. automatically get daily changes in KRS registry in order to refresh data for all updated entities.

## Config file
In order for the tool to work, attached .env.example file has to be filled with values that will tell the script where to point in order to conenct to i.e. Redis queue, PSQL Database resposible for storing raw data, trasnformed data, and log data. The name of the file should then be changed to .env.
If project is used in docker stack, some ip addresses can be left the way they are in the .env.example file. For example, `REDIS_HOST=redis://redis` will point to the addres of redis server container with name 'redis', that is in the same docker network as the rest of the stack


## Docker configuration
Attached Makefile has pre-configured commands that allow for running the stack in different configurations, such as:
- `sudo make run-base` - will run docker images that are necessary for backend api to work, such as redis server, fastapi backend, and single worker nodes for scrpaing KRS Financial Documents and KRS API json registrar.
- `sudo make run-spark-d` - will run only spark ETL job in detached mode
- `sudo make down-spark` - will stop and remove only spark container
## Future Updates
- Add task-level logging functionality to catch unexpected errors during
task scraping process
- Add analytics endpoint responsible for returning statistical data and analysis for specific business and comparison between businesses
- ~~Add docker file for composing images for fastapi server and scraping workers~~
