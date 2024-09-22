import contextlib
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

from sqlalchemy import MetaData, select

from bkp_db.create import create_session
from bkp_db.queries import (
    backup_destination_contents,
    files_to_backup,
    from_filepath,
    modified_files,
)
from bkp_db.types import BackupDest, BackupFile
from bkp_p.bkp_xml import BkpFile
from bkp_p.volume_id import VolumeId


class Bdsm:
    # Backup Database Syncing Mostly
    def __init__(self, db_directory=None) -> None:
        self.session, self.engine = create_session(db_directory)

    def init_dest(self, dest_id: VolumeId) -> None:
        """Initialise a destination
        Ensure a unique entry exists for the destination id

        Args:
            dest_id (VolumeId): The destination ID
        """
        result = self.session.query(BackupDest).filter(BackupDest.name == dest_id).all()

        if not result:
            result = BackupDest(name=dest_id)
            self.session.add(result)

    def clear_database(self):
        meta = MetaData()
        with contextlib.closing(self.engine.connect()) as con:
            trans = con.begin()
            for table in reversed(meta.sorted_tables):
                con.execute(table.delete())
            trans.commit()

    def update_file_entry(self, contents: BkpFile, path: Path):
        entry = from_filepath(self.session, path)
        entry.size = contents.size
        entry.timestamp = datetime.fromtimestamp(contents.mtime)
        entry.md5_hash = contents.md5
        # The modified tag says, does the source xml file
        # need updating at a later stage, given stuff we've done.
        entry.visited = 0

        # REVISIT - this could be more efficient
        entry.backup_dest.clear()
        for bd in contents.bkp_dests:
            # Backup destination contents
            bdc = backup_destination_contents(self.session, bd)
            entry.backup_dest.append(bdc)
        self.session.add(entry)
        self.session.commit()
        return entry

    def files_to_backup(self, bkp_id: VolumeId):
        return files_to_backup(self.session, bkp_id)

    def action_files_to_backup(
        self, bkp_id: VolumeId, callback: Callable[[BackupFile], None]
    ):
        for file_entry in self.files_to_backup(bkp_id):
            callback(file_entry)
            file_entry.visited = 1

    @property
    def modified_files(self) -> Iterable[BackupFile]:
        return modified_files(self.session)

    def action_modified_files(self, callback: Callable[[BackupFile], None]):
        for file_entry in self.modified_files:
            callback(file_entry)
            file_entry.visited = 0
