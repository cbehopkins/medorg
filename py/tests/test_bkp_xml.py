from pathlib import Path
from lxml import etree
from bkp_p.async_bkp_xml import AsyncBkpXmlManager
from bkp_p.bkp_xml import XML_NAME, BkpFile, BkpXml
import textwrap
from aiopath import AsyncPath

import pytest


def test_bkp_file_xml_render():
    expected_output = textwrap.dedent(
        """\
        <dr>
          <file fname="my file" mtime="256" size="128" checksum="deadbeef">
            <bd id="abc"/>
          </file>
        </dr>
        """
    )
    src = BkpFile(
        name="my file",
        file_path=Path("/home/test/"),
        size=128,
        mtime=256,
        md5="deadbeef",
        bkp_dests={"abc"},
    )
    root = etree.Element("dr")
    file_elem = etree.SubElement(root, "file")
    src.update_file_elem(file_elem)
    xml_data = etree.tostring(root, pretty_print=True, encoding="unicode")
    assert xml_data == expected_output


def test_bkp_file_xml_load(monkeypatch):
    source_input = textwrap.dedent(
        """\
        <dr>
          <fr fname="my_file" mtime="256" size="128" checksum="deadbeef">
            <bd id="def"/>
            <bd id="abc"/>
          </fr>
        </dr>
        """
    )

    def my_md5_calc(*_):
        raise RuntimeError

    monkeypatch.setattr(BkpXml, "_init_checks", lambda _: None)
    monkeypatch.setattr(BkpXml, "_file_exists", lambda *_: True)
    monkeypatch.setattr(BkpXml, "_file_size", lambda *_: 128)
    monkeypatch.setattr(BkpXml, "_file_timestamp", lambda *_: 256)
    monkeypatch.setattr(BkpXml, "_calculate_md5", my_md5_calc)

    bob = BkpXml(Path("Some/Path"))
    bob._root_from_string(source_input)
    the_file = bob["my_file"]
    assert the_file.md5 == "deadbeef"
    assert the_file.size == 128
    assert the_file.name == "my_file"
    assert the_file.bkp_dests == {"def", "abc"}
    assert the_file.bkp_dests == {"abc", "def"}


def test_bkp_file_xml_load_changed_file_size(monkeypatch):
    source_input = textwrap.dedent(
        """\
        <dr>
          <fr fname="my_file" mtime="256" size="128" checksum="deadbeef">
            <bd id="def"/>
            <bd id="abc"/>
          </fr>
        </dr>
        """
    )

    def my_md5_calc(*_):
        return "fresh beef"

    monkeypatch.setattr(BkpXml, "_init_checks", lambda _: None)
    monkeypatch.setattr(BkpXml, "_file_exists", lambda *_: True)
    monkeypatch.setattr(BkpXml, "_file_size", lambda *_: 129)
    monkeypatch.setattr(BkpXml, "_file_timestamp", lambda *_: 256)
    monkeypatch.setattr(BkpXml, "_calculate_md5", my_md5_calc)

    bob = BkpXml(Path("Some/Path"))
    bob._root_from_string(source_input)
    the_file = bob["my_file"]
    assert the_file.md5 == "fresh beef"
    assert the_file.size == 129
    assert the_file.name == "my_file"


def test_bkp_file_xml_load_changed_file_clears_backup_dests(): ...


@pytest.mark.asyncio
async def test_files_are_created_with_expected_content(tmp_path):
    my_dest = "here"
    subdir = AsyncPath(tmp_path) / "subdir1"
    await subdir.mkdir()
    async with AsyncBkpXmlManager() as xml_man:
        subdir1 = xml_man[subdir]
        await subdir1.init_structs()
        file1 = subdir1["file1.txt"]
        file1.md5 = "0123"
        file1.bkp_dests.add(my_dest)
        subdir1["file1.txt"] = file1
    xml_file = subdir / XML_NAME
    root = etree.parse(xml_file).getroot()
    file_elem = root.find(f".//fr[@fname='file1.txt']")
    assert len([c.tag for c in file_elem]) == 1
    assert file_elem[0].attrib["id"] == my_dest
