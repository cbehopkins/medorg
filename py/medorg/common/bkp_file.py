from dataclasses import dataclass, field
import os
from pathlib import Path
from typing import Self
import warnings

from lxml import etree

from medorg.common.io_boundary import (
    calculate_md5_for_existing_file,
    require_existing_file,
)
from medorg.common.types import Checksum, VolumeId
from .checksum import calculate_md5


@dataclass
class BkpFile:
    """This is our representation of a file in our XML_FILE
    You know the one we have one per directory.
    """

    name: str = None
    file_path: Path = None
    size: int = None
    mtime: int = None
    md5: Checksum = ""
    bkp_dests: set[VolumeId] = field(default_factory=set)

    def to_xml(self) -> str:
        file_elem = etree.Element()
        self.update_file_elem(file_elem)
        return etree.tounicode(file_elem, pretty_print=True)

    def _write_xml_attrs(self, file_elem: etree.Element) -> None:
        file_elem.set("fname", str(self.name))
        file_elem.set("mtime", str(self.mtime))
        file_elem.set("size", str(self.size))
        file_elem.set("checksum", self.md5)

    def _write_xml_children(self, file_elem: etree.Element) -> None:
        # Remove any current bd elements, before adding new ones...
        [file_elem.remove(elem) for elem in file_elem.xpath("bd")]

        if self.bkp_dests:
            for backup_dest in self.bkp_dests:
                bkp_elm = etree.SubElement(file_elem, "bd")
                bkp_elm.text = backup_dest

    def update_file_elem_strict(self, file_elem: etree.Element) -> Self:
        """Serialize this model without performing any filesystem I/O."""
        if not self.name:
            raise ValueError("name must be set")
        if self.mtime is None:
            raise ValueError("mtime must be set")
        if self.size is None:
            raise ValueError("size must be set")
        if not self.md5:
            raise ValueError("md5 must be set")

        self._write_xml_attrs(file_elem)
        self._write_xml_children(file_elem)
        return self

    def update_stat_result(self, stat_result_i: os.stat_result):
        current_size = stat_result_i.st_size

    def update_file_elem(self, file_elem: etree.Element) -> Self:
        if not self.name:
            raise ValueError("name must be set")
        if self.mtime is None:
            raise ValueError("mtime must be set")
        if self.size is None:
            raise ValueError("size must be set")
        if not self.md5:
            # Explicit sync boundary: this model method performs sync I/O.
            warnings.warn(
                "update_file_elem fallback md5 calculation is deprecated; "
                "prepopulate md5 and use strict serializers in async flows.",
                DeprecationWarning,
                stacklevel=2,
            )
            self.file_path = require_existing_file(self.file_path)
            self.md5 = calculate_md5_for_existing_file(self.file_path)
        self._write_xml_attrs(file_elem)
        self._write_xml_children(file_elem)
        return self

    @classmethod
    def from_file_elem(cls, file_elem: etree.Element, file_path: Path) -> Self:
        assert isinstance(file_path, Path)
        try:
            existing_timestamp = file_elem.get("mtime")
        except ValueError:
            existing_timestamp = float(file_elem.get("mtime"))
        if existing_timestamp is None:
            existing_timestamp = -1
        existing_timestamp = int(existing_timestamp)
        existing_size = int(file_elem.get("size", -1))
        md5_hash = file_elem.get("checksum")
        bkp_dests = {e.text for e in file_elem.xpath("bd")}
        assert None not in [
            existing_timestamp,
            existing_size,
            md5_hash,
        ], f"Invalid file_elem: {file_elem}"
        return cls(
            name=file_path.name,
            file_path=file_path,
            size=existing_size,
            mtime=existing_timestamp,
            md5=md5_hash,
            bkp_dests=bkp_dests,
        )
