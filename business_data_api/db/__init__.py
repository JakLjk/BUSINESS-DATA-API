import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

load_dotenv()

postgresql_url = os.getenv('POSTGRESQL_URL')

engine = create_engine(postgresql_url)
psql_session = sessionmaker(bind=engine)
Base = declarative_base()