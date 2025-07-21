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

## How to use the tool
### To get data  for specific company you need to know it's KRS number, which is unique number assigned to business entities registered in Poland's National Court Registrer.
1. Documentation for KRS API endpoints and their corresponding functions can be accessed by opening webpage: `<server ip>:<server port>/krs-api/docs`
2. Documentation for KRS API endpoints and their corresponding functions can be accessed by opening webpage: `<server ip>:<server port>/krs-df/docs`




## Future Updates
- Add task-level logging functionality to catch unexpected errors during
task scraping process
- Add docker file for composing images for fastapi server and scraping workers