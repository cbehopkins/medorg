import asyncio
import contextlib
import logging
import stat
from os import PathLike, stat_result
from xml.sax.saxutils import escape

from aiopath import AsyncPath
from lxml import etree

from medorg.common.bkp_file import BkpFile
from medorg.common.checksum import async_calculate_md5

from . import XML_NAME

_log = logging.getLogger(__name__)


class AsyncBkpXmlError(Exception):
    """Custom exception for AsyncBkpXml errors."""


class Counter:
    def __init__(self, initial=0):
        self._value = initial
        self.lock = asyncio.Lock()
        self._condition = asyncio.Condition()

    async def increment(self):
        async with self._condition:
            async with self.lock:
                self._value += 1
                self._condition.notify_all()

    async def decrement(self):
        async with self._condition:
            async with self.lock:
                self._value -= 1
                if self._value == 0:
                    self._condition.notify_all()

    async def wait_for_zero(self):
        async with self._condition:
            await self._condition.wait_for(lambda: self._value == 0)

BKP_XML_SCHEMA = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <!-- Define a complex type for the bd element -->
        <xs:complexType name="bdType" mixed="true">
            <xs:simpleContent>
                <xs:extension base="xs:string"/>
            </xs:simpleContent>
        </xs:complexType>

        <!-- Define a complex type for the tag element -->
        <xs:complexType name="tagType" mixed="true">
            <xs:simpleContent>
                <xs:extension base="xs:string"/>
            </xs:simpleContent>
        </xs:complexType>

        <!-- Define a complex type for the fr element -->
        <xs:complexType name="frType">
            <xs:sequence>
                <xs:element name="bd" type="bdType" minOccurs="0" maxOccurs="unbounded"/>
                <xs:element name="tag" type="tagType" minOccurs="0" maxOccurs="unbounded"/>
            </xs:sequence>
            <xs:attribute name="fname" type="xs:string" use="required"/>
            <xs:attribute name="mtime" type="xs:integer" use="required"/>
            <xs:attribute name="size" type="xs:integer" use="required"/>
            <xs:attribute name="checksum" type="xs:string" use="required"/>
        </xs:complexType>

        <!-- Define the root element dr -->
        <xs:element name="dr">
            <xs:complexType>
                <xs:sequence>
                    <xs:element name="fr" type="frType" minOccurs="0" maxOccurs="unbounded"/>
                </xs:sequence>
                <xs:attribute name="dir" type="xs:string" use="optional"/>
            </xs:complexType>
        </xs:element>
    </xs:schema>
        """


class AsyncBkpXml:
    def __init__(self, path: PathLike):
        self.path = AsyncPath(path)
        self.xml_path: AsyncPath = self.path / XML_NAME
        self.parser = etree.XMLParser(remove_blank_text=True)
        self.root = None
        self.lock = asyncio.Lock()
        self.schema = etree.XMLSchema(etree.XML(BKP_XML_SCHEMA))

    async def init_structs(self):
        async with self.lock:
            if self.root is not None:
                return

            if not await self.path.is_dir():
                raise FileNotFoundError(f"{self.path} does not exist as a directory")

            if await self.xml_path.exists():
                self.root = await self._root_from_file_path()
            else:
                self.root = etree.Element("dr")

    @staticmethod
    def _same_stats(cand: BkpFile, sr: stat_result):
        if cand.mtime != int(sr.st_mtime):
            return False
        if cand.size != sr.st_size:
            return False
        return True

    async def visit_file(self, entry: AsyncPath, sr: stat_result):
        # Visiting a file is saying:
        # This is a file I have found on disk, here's the current stat_results
        # Please update the xml as appropriate
        # it's guaranteed to exist
        # But we might have to (re) generate the md5
        # The fast path MUST be to go: yeah, it's what we expect from the xml
        # so do not create any new structs
        if self.path != entry.parent:
            _log.error(f"Path mismatch: expected {self.path}, got {entry.parent}")
        current_entry = self[entry.name]
        if not current_entry.md5 or not self._same_stats(current_entry, sr):
            try:
                new_md5 = await async_calculate_md5(entry)
            except Exception as e:
                _log.error(f"Failed to calculate MD5 for {entry}: {e}")
                raise AsyncBkpXmlError(f"Failed to calculate MD5 for {entry}") from e
            current_entry.size = sr.st_size
            current_entry.mtime = int(sr.st_mtime)
            current_entry.md5 = new_md5
            self[entry.name] = current_entry

    async def _populate_file(self, file_path: AsyncPath):
        """Task template to populate the file contents

        Args:
            file_path (AsyncPath): Path to populate

        Raises:
            FileNotFoundError: Asked to populate a file that doesn't exist
        """
        file_stats = await file_path.stat()
        if not stat.S_ISREG(file_stats.st_mode):
            raise FileNotFoundError(f"File {file_path} is in AsyncBkpXML but not found")
        await self.visit_file(file_path, file_stats)

    async def visit_all(self):
        async with self.lock:
            if self.root is None:
                raise RuntimeError("Root is None")

        tasks = []
        for file in self.root.findall(".//fr"):
            with contextlib.suppress(KeyError):
                if (
                    file.attrib["mtime"]
                    and file.attrib["size"]
                    and file.attrib["checksum"]
                ):
                    continue
            file_path = self.path / file.attrib["fname"]
            tasks.append(self._populate_file(file_path))
        await asyncio.gather(*tasks)

    async def _root_from_file_path(self):
        try:
            result: str = await self.xml_path.read_text()
            root = self._root_from_string(result)
            self._validate_xml(root)
            return root
        except Exception as e:
            _log.error(f"Failed to read XML from {self.xml_path}: {e}")
            return etree.Element("dr")

    def _root_from_string(self, xml_str: str):
        try:
            tree = etree.XML(xml_str, self.parser)
            assert tree is not None
            return tree
        except etree.XMLSyntaxError as e:
            _log.error(f"XML syntax error: {e}")
        except Exception as e:
            _log.error(f"Failed to parse XML string: {e}")
        return etree.Element("dr")

    def _validate_xml(self, root):
        if not self.schema.validate(root):
            log_msg = f"XML validation error: {self.schema.error_log}"
            _log.error(log_msg)
            raise AsyncBkpXmlError(log_msg)

    def _lkup_elem(self, key: str) -> etree.Element:
        escaped_key = escape(key)
        if "'" in escaped_key and '"' in escaped_key:
            # Replace single quotes with &apos; and use double quotes around the attribute value
            escaped_key = escaped_key.replace("'", "&apos;")
            return self.root.find(f'.//fr[@fname="{escaped_key}"]')
        if "'" in escaped_key:
            # Use double quotes around the attribute value
            return self.root.find(f'.//fr[@fname="{escaped_key}"]')

        # Use single quotes around the attribute value
        return self.root.find(f".//fr[@fname='{escaped_key}']")

    def __getitem__(self, key: str) -> BkpFile:
        """Get a file object requested file"""
        # FIXME before this is called we must have done all the io updates
        # and so this is just about doing self.root -> BkpFile conversion
        file_elem = self._lkup_elem(key)
        if file_elem is None:
            return BkpFile(
                name=key,
                file_path=(self.path / key),
                size=None,
                mtime=None,
            )
        file_path = self.path / key
        return BkpFile.from_file_elem(file_elem, file_path)

    def __setitem__(self, key: str, value: BkpFile) -> None:
        # This should only be about setting self.root <- BkpFile conversion
        assert self.root is not None
        file_elem = self._lkup_elem(key)
        if file_elem is None:
            file_elem = etree.SubElement(self.root, "fr")
        value.update_file_elem(file_elem)

    def _remove_if_not_in_set(self, file_set: set[str]) -> None:
        file: etree.Element
        for file in self.root.findall(".//fr"):
            name = file.attrib["fname"]
            if name not in file_set:
                file.getparent().remove(file)

    async def commit(self) -> None:
        try:
            await self.visit_all()
            self._validate_xml(self.root)

            xml_data = etree.tounicode(self.root, pretty_print=True)
            await self.xml_path.write_text(xml_data)
        except Exception as e:
            _log.error(f"Failed to write XML to {self.xml_path}: {e}")
            raise AsyncBkpXmlError(f"Failed to write XML to {self.xml_path}") from e


class AsyncBkpXmlManager(dict[AsyncPath, AsyncBkpXml]):
    def __init__(self) -> None:
        super().__init__()

    def __getitem__(self, key: AsyncPath) -> AsyncBkpXml:
        assert isinstance(key, AsyncPath), "You need to provide an AsyncPath object"
        if key not in self:
            self[key] = AsyncBkpXml(key)
        return super().__getitem__(key)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # FIXME do this with a tasks gather
        for bkp_xml in self.values():
            try:
                await bkp_xml.commit()
            except AsyncBkpXmlError as e:
                _log.error(f"Failed to commit changes for {bkp_xml.path}: {e}")
