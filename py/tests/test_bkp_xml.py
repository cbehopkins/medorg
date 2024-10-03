import textwrap
from pathlib import Path

import pytest
from aiopath import AsyncPath
from lxml import etree

from medorg.bkp_p import XML_NAME
from medorg.bkp_p.async_bkp_xml import AsyncBkpXmlManager
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
