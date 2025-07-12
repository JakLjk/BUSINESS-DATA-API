from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base


Base = declarative_base()


# Defining sync objects for PostgreSQL
def create_sync_sessionmaker(psql_sync_url):
    sync_engine = create_engine(psql_sync_url)
    psql_sync_session = sessionmaker(bind=sync_engine)
    return psql_sync_session

# Defining async objects for PostgreSQL
def create_async_sessionmaker(psql_async_url):
    async_engine = create_async_engine(
                                psql_async_url, 
                                echo=False
                                )
    psql_async_session = sessionmaker(
                                bind=async_engine,
                                class_=AsyncSession,
                                expire_on_commit=False
                                )
    return psql_async_session

def create_tables(psql_sync_url):
    sync_engine = create_engine(psql_sync_url)
    Base.metadata.create_all(bind=sync_engine)
