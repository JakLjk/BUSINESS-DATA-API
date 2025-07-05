import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# Loading environmental variables
load_dotenv()
postgresql_url = os.getenv('ASYNC_POSTGRESQL_URL')
sync_postgresql_url = os.getenv("SYNC_POSTGRE_URL")

# Defining sync objects for PostgreSQL
sync_engine = create_engine(
    sync_postgresql_url)
psql_sync_session = sessionmaker(
    bind=sync_engine)
# Defining async objects for PostgreSQL
async_engine = create_async_engine(
                            postgresql_url, 
                            echo=False
                            )
psql_async_session = sessionmaker(
                            bind=async_engine,
                            class_=AsyncSession,
                            expire_on_commit=False
                            )
Base = declarative_base()