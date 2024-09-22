import logging
import re
from pathlib import Path
from uuid import uuid4

import aiofiles

from bkp_p.exceptions import DestIdException, VolumeIdNotGenerated

_log = logging.getLogger(__name__)
VolumeId = str


class VolumeIdSrc:
    def __init__(self, path: Path):
        self.path = path
        self._volume_id = None

    @property
    def volume_id(self) -> VolumeId:
        if self._volume_id is None:
            raise VolumeIdNotGenerated
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

    @staticmethod
    def validate_uuid(uuid: VolumeId):
        uuid_re = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
        pattern = re.compile(uuid_re)
        return pattern.match(uuid)

    @staticmethod
    def create_dest_id() -> VolumeId:
        # FIXME add code to check if this is already in the database
        return str(uuid4())
