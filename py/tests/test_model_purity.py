import warnings

import pytest
from aiopath import AsyncPath
from lxml import etree

import medorg.common.io_boundary as io_boundary
from medorg.bkp_p.async_bkp_xml import AsyncBkpXmlManager
from medorg.common.bkp_file import BkpFile


def test_update_file_elem_strict_does_not_use_io_boundary(monkeypatch):
    def fail_require(*args, **kwargs):
        raise AssertionError("strict serializer should not resolve file paths")

    def fail_md5(*args, **kwargs):
        raise AssertionError("strict serializer should not calculate md5")

    monkeypatch.setattr(io_boundary, "require_existing_file", fail_require)
    monkeypatch.setattr(io_boundary, "calculate_md5_for_existing_file", fail_md5)

    src = BkpFile(
        name="my-file.txt",
        file_path=AsyncPath("/tmp/my-file.txt"),
        size=128,
        mtime=256,
        md5="deadbeef",
        bkp_dests={"dest-a"},
    )
    root = etree.Element("dr")
    file_elem = etree.SubElement(root, "fr")

    src.update_file_elem_strict(file_elem)

    assert file_elem.get("fname") == "my-file.txt"
    assert file_elem.get("mtime") == "256"
    assert file_elem.get("size") == "128"
    assert file_elem.get("checksum") == "deadbeef"


@pytest.mark.asyncio
@pytest.mark.filterwarnings(
    "error:coroutine 'AsyncPath.stat' was never awaited:RuntimeWarning"
)
async def test_async_bkpxml_strict_setitem_no_asyncpath_warning(tmp_path):
    async_root = AsyncPath(tmp_path)
    subdir = async_root / "subdir1"
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
            file1 = subdir_xml["file1.txt"]
            file1.md5 = "0123"
            file1.size = 1
            file1.mtime = 0
            file1.bkp_dests.add("dest-a")
            subdir_xml["file1.txt"] = file1
