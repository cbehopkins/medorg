from dataclasses import dataclass, field
from pathlib import Path
from typing import Self

from lxml import etree

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
    md5: str = ""
    bkp_dests: set[str] = field(default_factory=set)

    def update_file_elem(self, file_elem: etree.Element) -> Self:
        file_elem.set("fname", str(self.name))
        file_elem.set("mtime", str(self.mtime))
        file_elem.set("size", str(self.size))
        if not self.md5:
            if not self.file_path.is_file():
                raise FileNotFoundError(f"File {self.file_path} not found")
            self.md5 = calculate_md5(str(self.file_path))
        file_elem.set("checksum", self.md5)
        # Remove any current bd elements, before adding new ones...
        [file_elem.remove(elem) for elem in file_elem.xpath("bd")]

        if self.bkp_dests:
            for backup_dest in self.bkp_dests:
                bkp_elm = etree.SubElement(file_elem, "bd")
                bkp_elm.set("id", backup_dest)
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
        bkp_dests = {e.attrib["id"] for e in file_elem.xpath("bd[@id]")}
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
