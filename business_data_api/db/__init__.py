import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

load_dotenv()

postgresql_url = os.getenv('POSTGRESQL_URL')
sync_postgresql_url = os.getenv("SYNC_POSTGRE_URL")

syncengine = create_engine(sync_postgresql_url)
psql_syncsession = sessionmaker(bind=syncengine)

aengine = create_async_engine(postgresql_url)
psql_asession = sessionmaker(bind=aengine,
                            class_=AsyncSession,
                            expire_on_commit=True)
Base = declarative_base()