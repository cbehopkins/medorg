from sqlalchemy import func

from .types import BackupDest, BackupFile


def modified_files(session):
    return session.query(BackupFile).filter(BackupFile.visited == 1).all()


def from_filepath(session, path) -> BackupFile:
    entry = session.query(BackupFile).filter_by(filename=str(path)).first()
    if entry is None:
        entry = BackupFile(filename=str(path))
    return entry


def backup_destination_contents(session, bd):
    bdc = session.query(BackupDest).filter_by(name=str(bd)).first()
    if bdc is None:
        bdc = BackupDest(name=bd)
    return bdc


def files_to_backup(session, dest_id):
    return (
        session.query(BackupFile)
        # Remove any files that already have the current dest_id
        # .filter(~BackupFile.backup_dest.any(BackupDest.name == dest_id))
        .order_by(func.length(BackupFile.backup_dest))
        .order_by(BackupFile.size.desc())
        .all()
    )
