import base64
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Self
from lxml import etree

XML_NAME = ".bkp.xml"


def calculate_md5(file_path):
    """Calculate MD5 hash for a given file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    tmp = base64.b64encode(hash_md5.digest())
    if len(tmp) == 24 and tmp[22:24] == b"==":
        return tmp[:22]
    return tmp


@dataclass
class BkpFile:
    name: str = None
    file_path: Path = None
    size: int = None
    mtime: int = None
    md5: str = ""
    bkp_dests: set[str] = field(default_factory=set)

    def update_file_elem(self, file_elem):
        file_elem.set("fname", str(self.name))
        file_elem.set("mtime", str(self.mtime))
        file_elem.set("size", str(self.size))
        if self.md5 == "":
            self.md5 = calculate_md5(str(self.file_path))
        file_elem.set("checksum", self.md5)
        # Remove any current bd elements, before adding new ones...
        list(file_elem.remove(elem) for elem in file_elem.xpath("bd"))
        if self.bkp_dests:
            for backup_dest in self.bkp_dests:
                bkp_elm = etree.SubElement(file_elem, "bd")
                bkp_elm.set("id", backup_dest)
        return self

    @classmethod
    def from_file_elem(cls, file_elem, file_path: Path) -> Self:
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
        bkpf = cls(
            name=file_path.name,
            file_path=file_path,
            size=existing_size,
            mtime=existing_timestamp,
            md5=md5_hash,
            bkp_dests=bkp_dests,
        )
        return bkpf


class BkpXml:
    def __init__(self, path: Path):
        self.path = path
        self.xml_path = Path(path) / XML_NAME
        self._init_checks()

        if self.xml_path.exists():
            self._root_from_file_path()
        else:
            self.root = etree.Element("dr")
        self._files: dict[str, BkpFile] = {}

    def _init_checks(self):
        if not self.path.is_dir():
            raise FileNotFoundError(f"{self.path} does not exist as a directory")

    @staticmethod
    def _file_exists(path: Path):
        return path.is_file()

    @staticmethod
    def _file_timestamp(path: Path):
        return int(path.stat().st_mtime)

    @staticmethod
    def _file_size(path: Path):
        return path.stat().st_size

    @staticmethod
    def _calculate_md5(path: Path):
        return calculate_md5(str(path))

    def _root_from_file_path(self):
        parser = etree.XMLParser(remove_blank_text=True)
        tree = etree.parse(str(self.xml_path), parser)
        self.root = tree.getroot()

    def _root_from_string(self, xml_str: str):
        parser = etree.XMLParser(remove_blank_text=True)
        tree = etree.fromstring(xml_str, parser)
        self.root = tree

    def __getitem__(self, key: str) -> BkpFile:
        """Get a file object for the directory"""
        if not self._file_exists(self.path / key):
            if key in self._files:
                del self._files[key]
            raise FileNotFoundError(str(self.path / key))

        if key not in self._files:
            file_elem = self.root.find(f".//fr[@fname='{key}']")
            if file_elem is not None:
                self._files[key] = self._from_file_elem(file_elem, key)
            else:
                self._files[key] = self._from_file(key)
        return self._files[key]

    def __setitem__(self, key: str, value: BkpFile) -> None:
        if not (self.path / key).is_file():
            if key in self._files:
                del self._files[key]
            raise FileNotFoundError(str(self.path / key))
        assert isinstance(value, BkpFile)
        self._files[key] = value

    def _from_file_elem(self, file_elem, key):
        file_path = self.path / key
        current_timestamp = self._file_timestamp(file_path)
        current_size = self._file_size(file_path)
        bkpf = BkpFile.from_file_elem(file_elem, file_path)
        existing_timestamp = bkpf.mtime
        existing_size = bkpf.size
        if existing_timestamp != current_timestamp or existing_size != current_size:
            # Recalculate MD5 hash
            bkpf.md5 = self._calculate_md5(file_path)
            bkpf.mtime = current_timestamp
            bkpf.size = current_size
        return bkpf

    def _from_file(self, key):
        file_path = self.path / key
        # File entry does not exist, create a new one
        md5_hash = calculate_md5(str(file_path.resolve()))
        current_timestamp = int(file_path.stat().st_mtime)
        current_size = file_path.stat().st_size
        file_elem = etree.SubElement(self.root, "fr")
        return BkpFile(
            size=current_size,
            name=key,
            file_path=file_path,
            mtime=current_timestamp,
            md5=md5_hash,
        ).update_file_elem(file_elem)

    def purge(self):
        for file in self.root.findall(".//fr"):
            name = file.attrib["name"]
            if not (self.path / name).is_file():
                print("Removing file that doesn't exist")
                file.getparent.remove(file)

    def commit(self):
        for filename, bkp in self._files.items():
            file_elem = self.root.find(f".//fr[@fname='{filename}']")
            if file_elem is None:
                raise Exception(
                    f"{filename} was not found even though we modified it!!!"
                )
            bkp.update_file_elem(file_elem)

        xml_data = etree.tostring(self.root, pretty_print=True, encoding="unicode")
        self.xml_path.write_text(xml_data)

    @classmethod
    def update_path(cls, path):
        tmp = cls(path)
        tmp.update()

    def update(self):
        self.purge()
        for file_path in self.path.glob("*"):
            if file_path.is_dir():
                self.update_path(file_path)
            if not file_path.is_file():
                continue
            file_name = file_path.name
            if file_name == XML_NAME:
                continue  # Skip the .bkp.xml file itself
            self[file_name]
        self.commit()

class BkpXmlManager(dict[Path, BkpXml]):
    def __init__(self) -> None:
        pass

    def __getitem__(self, key: Path) -> BkpXml:
        assert isinstance(key, Path), "You need to provide a path object"
        key = key.resolve()
        if key not in self:
            self[key] = BkpXml(key)
        return super().__getitem__(key)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for bkp_xml in self.values():
            try:
                bkp_xml.commit()
            except Exception as e:
                print("Here")
