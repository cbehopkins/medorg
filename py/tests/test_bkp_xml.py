import os
import textwrap
from pathlib import Path
import warnings

import pytest
from aiopath import AsyncPath
from lxml import etree

from medorg.bkp_p.async_bkp_xml import AsyncBkpXml, AsyncBkpXmlManager
from medorg.bkp_p.backup_xml_walker import BackupXmlWalker
from medorg.common import XML_NAME
from medorg.common.bkp_file import BkpFile
from medorg.common.checksum import calculate_md5


def test_bkp_file_xml_render():
    expected_output = textwrap.dedent(
        """\
        <dr>
          <file fname="my file" mtime="256" size="128" checksum="deadbeef">
            <bd>abc</bd>
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


def test_bkp_file_xml_render_strict():
    expected_output = textwrap.dedent(
        """\
        <dr>
          <file fname="my file" mtime="256" size="128" checksum="deadbeef">
            <bd>abc</bd>
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
    src.update_file_elem_strict(file_elem)
    xml_data = etree.tostring(root, pretty_print=True, encoding="unicode")
    assert xml_data == expected_output


def test_bkp_file_strict_requires_md5():
    src = BkpFile(
        name="my file",
        file_path=Path("/home/test/"),
        size=128,
        mtime=256,
        md5="",
        bkp_dests={"abc"},
    )
    root = etree.Element("dr")
    file_elem = etree.SubElement(root, "file")
    with pytest.raises(ValueError, match="md5 must be set"):
        src.update_file_elem_strict(file_elem)


@pytest.mark.parametrize(
    ("kwargs", "match_text"),
    [
        ({"name": "", "size": 128, "mtime": 256, "md5": "deadbeef"}, "name"),
        (
            {"name": "my file", "size": 128, "mtime": None, "md5": "deadbeef"},
            "mtime",
        ),
        (
            {"name": "my file", "size": None, "mtime": 256, "md5": "deadbeef"},
            "size",
        ),
        ({"name": "my file", "size": 128, "mtime": 256, "md5": ""}, "md5"),
    ],
)
def test_bkp_file_strict_required_fields_matrix(kwargs, match_text):
    src = BkpFile(
        name=kwargs["name"],
        file_path=Path("/home/test/"),
        size=kwargs["size"],
        mtime=kwargs["mtime"],
        md5=kwargs["md5"],
        bkp_dests={"abc"},
    )
    root = etree.Element("dr")
    file_elem = etree.SubElement(root, "file")
    with pytest.raises(ValueError, match=match_text):
        src.update_file_elem_strict(file_elem)


def test_bkp_file_legacy_fallback_emits_deprecation_warning(tmp_path):
    data_file = tmp_path / "data.txt"
    data_file.write_text("hello")

    src = BkpFile(
        name="data.txt",
        file_path=data_file,
        size=data_file.stat().st_size,
        mtime=int(data_file.stat().st_mtime),
        md5="",
        bkp_dests={"abc"},
    )
    root = etree.Element("dr")
    file_elem = etree.SubElement(root, "file")

    with pytest.warns(DeprecationWarning, match="deprecated"):
        src.update_file_elem(file_elem)

    assert src.md5 == calculate_md5(str(data_file))


@pytest.mark.asyncio
@pytest.mark.filterwarnings("error:coroutine 'AsyncPath.stat' was never awaited:RuntimeWarning")
async def test_async_bkpxml_strict_roundtrip_no_asyncpath_warning(tmp_path):
    subdir = AsyncPath(tmp_path) / "subdir_roundtrip"
    await subdir.mkdir(parents=True, exist_ok=True)

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "error",
            message="coroutine 'AsyncPath.stat' was never awaited",
            category=RuntimeWarning,
        )
        async with AsyncBkpXmlManager() as xml_man:
            subdir_xml = xml_man[subdir]
            await subdir_xml.init_structs()
            entry = subdir_xml["file1.txt"]
            entry.md5 = "0123"
            entry.size = 1
            entry.mtime = 0
            entry.bkp_dests = {"dest-a", "dest-b"}
            subdir_xml["file1.txt"] = entry

    axml = AsyncBkpXml(path=subdir)
    await axml.init_structs()
    loaded = axml["file1.txt"]
    assert loaded.md5 == "0123"
    assert loaded.size == 1
    assert loaded.mtime == 0
    assert loaded.bkp_dests == {"dest-a", "dest-b"}


def test_example_xml(tmp_path):
    example = textwrap.dedent(
        """\
    <dr>
        <fr fname="file2.txt" mtime="1728309119" size="150" checksum="0n4+rJ6OUsZkd321mIiHIw">
            <bd>some first destination</bd>
            <tag>some tag</tag>
        </fr>
        <fr fname="file1.txt" mtime="1728309119" size="100" checksum="38vYdOGOyHkmWoYX/+gv7A">
            <bd>some first destination</bd>
            <bd>some second destination</bd>
        </fr>
    </dr>
    """
    )
    axml = AsyncBkpXml(path=tmp_path)
    axml.root = axml._tree_from_string(example)
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


def write_random_data_to_files(paths):
    for path in paths:
        # Generate random data (e.g., 50 bytes)
        random_data = os.urandom(50)

        # Write the random data to the file
        with open(path, "wb") as file:
            file.write(random_data)


@pytest.mark.asyncio
async def test_possible_paths_xml(tmp_path):
    interesting_paths = [
        "file1.txt",
        "file2.txt",
        "Chris's file.txt",
        "nothin_here.txt" if os.name == "nt" else 'file with "quotes".txt',
    ]
    write_random_data_to_files([tmp_path / path for path in interesting_paths])
    walker = BackupXmlWalker(tmp_path)
    await walker.go_walk(walker=None)
    assert (tmp_path / XML_NAME).is_file()
    axml = AsyncBkpXml(path=tmp_path)
    await axml.init_structs()
    for file in interesting_paths:
        obj = axml[file]
        assert obj.size == 50
        assert obj.md5 is not None
        assert obj.mtime is not None
        assert obj.bkp_dests == set()


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
    assert file_elem[0].text == my_dest

