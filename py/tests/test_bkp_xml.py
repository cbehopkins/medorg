import textwrap
from pathlib import Path

import pytest
from aiopath import AsyncPath
from lxml import etree

from medorg.bkp_p import XML_NAME
from medorg.bkp_p.async_bkp_xml import AsyncBkpXml, AsyncBkpXmlManager
from medorg.common.bkp_file import BkpFile


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


def test_example_xml(tmp_path):
    example = textwrap.dedent(
        """\
    <dr>
        <fr fname="file2.txt" mtime="1728309119" size="150" checksum="0n4+rJ6OUsZkd321mIiHIw">
            <bd id="some first destination"/>
        </fr>
        <fr fname="file1.txt" mtime="1728309119" size="100" checksum="38vYdOGOyHkmWoYX/+gv7A">
            <bd id="some first destination"/>
            <bd id="some second destination"/>
        </fr>
    </dr>
    """
    )
    axml = AsyncBkpXml(path=tmp_path)
    axml.root = axml._root_from_string(example)
    axml._validate_xml(axml.root)
    assert axml["file1.txt"].size == 100
    assert axml["file1.txt"].bkp_dests == {
        "some first destination",
        "some second destination",
    }
    assert axml["file2.txt"].size == 150
    assert axml["file2.txt"].bkp_dests == {
        "some first destination",
    }


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
        file1.size = 1
        file1.mtime = 0
        file1.bkp_dests.add(my_dest)
        subdir1["file1.txt"] = file1
    xml_file = subdir / XML_NAME
    root = etree.parse(xml_file).getroot()
    file_elem = root.find(".//fr[@fname='file1.txt']")
    assert len([c.tag for c in file_elem]) == 1
    assert file_elem[0].attrib["id"] == my_dest
