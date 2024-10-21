import logging
import os
import re
from pathlib import Path
from uuid import uuid4

import aiofiles

from medorg.common.exceptions import DestIdException
from medorg.common.types import VolumeId

_log = logging.getLogger(__name__)


class VolumeIdSrc:

    def __init__(self, path: os.PathLike) -> None:
        self.path = Path(path)
        self._volume_id = None

    @property
    def volume_id(self) -> VolumeId:
        if self._volume_id is None:
            self.read_or_create_dest_id_sync()
        return self._volume_id

    @property
    async def avolume_id(self) -> VolumeId:
        if self._volume_id is None:
            await self.read_or_create_dest_id()
        return self._volume_id

    def __repr__(self) -> str:
        return f"VolumeId[{self.path}]"

    async def read_or_create_dest_id(self):
        dest_id_file = self.path / ".bkp_id"

        if dest_id_file.exists():
            try:
                async with aiofiles.open(dest_id_file, "r") as f:
                    contents = await f.read()
                    dest_id = contents.strip()
            except Exception as e:
                raise DestIdException from e
        else:
            dest_id = self.create_dest_id()
            try:
                async with aiofiles.open(dest_id_file, "w") as f:
                    await f.write(dest_id)
            except Exception as e:
                raise DestIdException from e
        assert self.validate_uuid(dest_id)
        self._volume_id = dest_id

    def read_or_create_dest_id_sync(self):
        dest_id_file = self.path / ".bkp_id"

        if dest_id_file.exists():
            try:
                with open(dest_id_file, "r") as f:
                    contents = f.read()
                    dest_id = contents.strip()
            except Exception as e:
                raise DestIdException from e
        else:
            dest_id = self.create_dest_id()
            try:
                with open(dest_id_file, "w") as f:
                    f.write(dest_id)
            except Exception as e:
                raise DestIdException from e
        assert self.validate_uuid(dest_id)
        self._volume_id = dest_id

    @staticmethod
    def validate_uuid(uuid: VolumeId):
        uuid_re = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
        pattern = re.compile(uuid_re)
        return pattern.match(uuid)

    @staticmethod
    def create_dest_id() -> VolumeId:
        # FIXME add code to check if this is already in the database
        return str(uuid4())
