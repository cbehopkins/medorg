import typing

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Table
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, declarative_base, relationship

VolumeId = str
Checksum = str

DatabaseBase = typing.Type[declarative_base]
# Base: DatabaseBase = declarative_base()


class Base(AsyncAttrs, DeclarativeBase):
    pass


class BackupSrc(Base):
    __tablename__ = "src_dirs"

    id = Column(Integer, primary_key=True)
    path = Column(String, nullable=False, unique=True)


# Define the association table for the many-to-many relationship
file_backup_dest_association = Table(
    "file_backup_dest_association",
    Base.metadata,
    Column("file_id", Integer, ForeignKey("backup_files.id")),
    Column("backup_dest_id", Integer, ForeignKey("backup_dest.id")),
)


class BackupFile(Base):
    __tablename__ = "backup_files"

    id = Column(Integer, primary_key=True)
    filename = Column(String, nullable=False, unique=True)
    src_path = Column(String, nullable=False)
    size = Column(Integer)
    timestamp = Column(DateTime)  # Use DateTime type for timestamp
    md5_hash = Column(String)
    visited = Column(Integer, default=0)

    backup_dest = relationship(
        "BackupDest",
        secondary=file_backup_dest_association,
        back_populates="files",
        lazy="joined",
    )

    def __str__(self) -> str:
        return f"{self.filename}:{self.md5_hash=}"

    @property
    def dest_names(self) -> set[str]:
        return {bd.name for bd in self.backup_dest}


class BackupDest(Base):
    """The destination for a backup as a database"""

    __tablename__ = "backup_dest"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    files = relationship(
        "BackupFile",
        secondary=file_backup_dest_association,
        back_populates="backup_dest",
    )
