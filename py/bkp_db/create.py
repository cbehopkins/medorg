import logging
import sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .types import Base

_log = logging.getLogger(__name__)


def create_database(db_directory: Path = None):
    if db_directory is None:
        db_directory = Path.home() / ".bkp_base"
    db_directory.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists
    db_path = db_directory / "backup_database.db"

    if not db_path.exists():
        try:
            engine = create_engine(f"sqlite:///{db_path}")
            Base.metadata.create_all(engine)
            return engine
        except Exception as e:
            print(f"Error creating database: {e}")
            sys.exit(1)
    else:
        return create_engine(f"sqlite:///{db_path}")


def create_session(db_directory):
    engine = create_database(db_directory)
    Session = sessionmaker(bind=engine)
    return Session(), engine
