import asyncio
import logging
from csv import excel
from os import PathLike, stat_result

from aiopath import AsyncPath
from lxml import etree

from medorg.common.bkp_file import BkpFile
from medorg.common.checksum import async_calculate_md5

from . import XML_NAME

_log = logging.getLogger(__name__)


class AsyncBkpXml:
    def __init__(self, path: PathLike):
        self.path = AsyncPath(path)
        self.xml_path: AsyncPath = self.path / XML_NAME
        self.parser = etree.XMLParser(remove_blank_text=True)
        self._files: dict[str, BkpFile] = {}
        self.root = None
        self.lock = asyncio.Lock()

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
                assert self.root is not None

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
            _log.error(f"{self.path=}::{entry.parent=} werid path base")
        current_entry = self[entry.name]
        # assert current_entry
        if not current_entry.md5 or not self._same_stats(current_entry, sr):
            new_md5 = await async_calculate_md5(entry)
            current_entry.size = sr.st_size
            current_entry.mtime = int(sr.st_mtime)
            current_entry.md5 = new_md5
            self[entry.name] = current_entry

    async def _root_from_file_path(self):
        # read in the file, then construct from string
        result: str = await self.xml_path.read_text()
        return self._root_from_string(result)

    def _root_from_string(self, xml_str: str):
        tree = etree.fromstring(xml_str, self.parser)
        assert tree is not None
        return tree

    def __getitem__(self, key: str) -> BkpFile:
        """Get a file object for the directory"""
        # FIXME before this is called we must have done all the io updates
        # and so this is just about doing self.root -> BkpFile conversion
        file_elem = self.root.find(f".//fr[@fname='{key}']")
        if file_elem is None:
            return BkpFile(
                name=key,
                file_path=(self.path / key),
                size=None,
                mtime=None,
            )
        return self._from_file_elem(file_elem, key)

    def __setitem__(self, key: str, value: BkpFile) -> None:
        # This should only be about setting self.root <- BkpFile conversion
        assert self.root is not None
        file_elem = self.root.find(f".//fr[@fname='{key}']")
        if file_elem is None:
            file_elem = etree.SubElement(self.root, "fr")
        value.update_file_elem(file_elem)

    def _from_file_elem(self, file_elem, key) -> BkpFile:
        # FIXME move to use accessor methods from bkp_xml
        file_path = self.path / key

        return BkpFile.from_file_elem(file_elem, file_path)

    def remove_if_not_in_set(self, file_set: set[str]) -> None:
        file: etree.Element
        for file in self.root.findall(".//fr"):
            name = file.attrib["fname"]
            if name not in file_set:
                file.getparent().remove(file)

    async def commit(self) -> None:
        if self.root is None:
            raise SystemError("self.root should not be none. Puzzled...")
        xml_data = etree.tostring(self.root, pretty_print=True, encoding="unicode")
        await self.xml_path.write_text(xml_data)


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
            await bkp_xml.commit()
