import datetime
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine, Column, String, Integer, DateTime
from logging import Handler, LogRecord

Base = declarative_base()

class BusinessDataApiLogs(Base):
    """ Table model for log data """
    __tablename__="business_data_api_logs"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow())
    level = Column(String)
    logger_name = Column(String)
    message = Column(String)


class PostgreSQLHandler(Handler):
    """ Handler for storing log data in PostgreSQL"""
    def __init__(self, postgresql_url:str):
        super().__init__()
        # Declaring db engine
        self.engine = create_engine(postgresql_url)
        # Creaing Log table in DB if it does not yet exists
        Base.metadata.create_all(self.engine)
        # Declaring db session
        self.session = sessionmaker(bind=self.engine)

    def emit(self, record:LogRecord):
        session = self.session()
        # creating record with log data
        log_entry = BusinessDataApiLogs(
            timestamp=datetime.datetime.fromtimestamp(record.created),
            level = record.levelname,
            logger_name = record.name,
            message = record.getMessage()
        )
        try:
            # trying to add log to DB
            session.add(log_entry)
            session.commit()
        except Exception:
            # In case of error, let the Handler take care of it
            self.handleError(record)
        finally:
            session.close()