from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

from lxml import etree

from medorg.common.types import BackupFile


@dataclass
class RestoreFile:
    name: str
    size: int
    file_path: Path = None
    mtime: int = None
    md5: str = ""
    bkp_dests: set[str] = field(default_factory=set)

    def __eq__(self, other):
        if not isinstance(other, RestoreFile):
            return False
        return (
            self.name == other.name
            and self.size == other.size
            and self.md5 == other.md5
        )

    def to_element(self) -> etree.Element:
        file_element = etree.Element(
            "file",
            name=self.name,
            size=str(self.size),
            mtime=str(self.mtime),
            md5=self.md5,
        )
        for dest in self.bkp_dests:
            dest_element = etree.Element("bd")
            dest_element.text = dest
            file_element.append(dest_element)
        return file_element

    @staticmethod
    def from_element(element: etree.Element) -> "RestoreFile":
        name = element.get("name")
        size = int(element.get("size"))
        mtime_i = element.get("mtime")
        mtime = int(mtime_i) if mtime_i != "None" else None
        md5 = element.get("md5")
        bkp_dests = {dest.text for dest in element.findall("bd")}
        return RestoreFile(
            name=name, size=size, mtime=mtime, md5=md5, bkp_dests=bkp_dests
        )


class RestoreDirectory:
    def __init__(self, name: str):
        self.name = name
        self.files: list[RestoreFile] = []
        self.subdirectories: list["RestoreDirectory"] = []

    def add_file(self, file: RestoreFile):
        self.files.append(file)

    def add_subdirectory(self, subdirectory: "RestoreDirectory"):
        self.subdirectories.append(subdirectory)

    def __eq__(self, other):
        if not isinstance(other, RestoreDirectory):
            return False
        return (
            self.name == other.name
            and sorted(self.files, key=lambda x: x.name)
            == sorted(other.files, key=lambda x: x.name)
            and sorted(self.subdirectories, key=lambda x: x.name)
            == sorted(other.subdirectories, key=lambda x: x.name)
        )

    def to_element(self) -> etree.Element:
        dir_element = etree.Element("dr", name=self.name)
        for file in self.files:
            dir_element.append(file.to_element())
        for subdirectory in self.subdirectories:
            dir_element.append(subdirectory.to_element())
        return dir_element

    @staticmethod
    def from_element(element: etree.Element) -> "RestoreDirectory":
        name = element.get("name")
        directory = RestoreDirectory(name=name)
        for file_element in element.findall("file"):
            directory.add_file(RestoreFile.from_element(file_element))
        for subdir_element in element.findall("dr"):
            directory.add_subdirectory(RestoreDirectory.from_element(subdir_element))
        return directory


class RestoreContext:
    # FIXME define a bdsa protocol
    # FIXME add __str__ and __repr__ methods
    def __init__(self, bdsa: Any):
        self.bdsa = bdsa  # FIXME this shoould not be part of the class - it should be passed into build_file_structure
        self.file_structure: dict[str, RestoreDirectory] = {}

    async def build_file_structure(self) -> dict[str, RestoreDirectory]:
        for backup_file in await self.bdsa.aquery_generator(BackupFile):
            src_path = backup_file.src_path
            path = Path(backup_file.filename)
            parts = list(path.parts)
            if src_path not in self.file_structure:
                self.file_structure[src_path] = RestoreDirectory(src_path)

            current_level = self.file_structure[src_path]

            for part in parts[:-1]:
                subdir = next(
                    (d for d in current_level.subdirectories if d.name == part), None
                )
                if subdir is None:
                    subdir = RestoreDirectory(part)
                    current_level.add_subdirectory(subdir)
                current_level = subdir

            # At the leaf node, create a RestoreFile object
            restore_file = RestoreFile(
                name=Path(backup_file.filename).name,
                size=backup_file.size,
                file_path=None,  # Assuming file_path is not needed here
                mtime=backup_file.timestamp,
                md5=backup_file.md5_hash,
                bkp_dests=set(backup_file.dest_names),
            )
            current_level.add_file(restore_file)

        return self.file_structure

    def to_element(self) -> etree.Element:
        root = etree.Element("root")
        for src_path, directory in self.file_structure.items():
            rc_element = etree.Element("rc", src_path=src_path)
            for file in directory.files:
                rc_element.append(file.to_element())
            for subdirectory in directory.subdirectories:
                rc_element.append(subdirectory.to_element())
            root.append(rc_element)
        return root

    def to_xml_string(self) -> str:
        return etree.tostring(self.to_element(), pretty_print=True, encoding="unicode")

    @staticmethod
    def from_element(element: etree.Element) -> "RestoreContext":
        context = RestoreContext(bdsa=None)  # bdsa is not needed for from_xml
        for rc_element in element.findall("rc"):
            src_path = rc_element.get("src_path")
            rc = RestoreDirectory(name=src_path)
            for subdirectory in rc_element.findall("dr"):
                directory = RestoreDirectory.from_element(subdirectory)
                rc.add_subdirectory(directory)
            context.file_structure[src_path] = rc
        return context

    @staticmethod
    def from_file_handle(file_handle: IO) -> "RestoreContext":
        """Construct a RestoreContext from an I/O stream.

        Args:
            file_handle (IO): An opened file handle containing the XML data.

        Returns:
            RestoreContext: The constructed RestoreContext object.
        """
        ## Load and parse the XML schema
        # with open(schema_file, 'r') as schema_fh:
        #     schema_root = etree.parse(schema_fh)
        #     schema = etree.XMLSchema(schema_root)
        tree = etree.parse(file_handle)
        root = tree.getroot()
        # Validate the XML against the schema
        # if not schema.validate(root):
        #     raise ValueError(f"XML does not conform to the schema: {schema.error_log}")

        return RestoreContext.from_element(root)
