import textwrap
from lxml import etree
import pytest

from medorg.restore.structs import RestoreContext, RestoreDirectory, RestoreFile


def test_restore_file_to_xml():
    # Create a RestoreFile instance
    restore_file = RestoreFile(
        name="example.txt",
        size=1024,
        file_path=None,
        mtime=1633036800,
        md5="d41d8cd98f00b204e9800998ecf8427e",
        bkp_dests={"backup1", "backup2"},
    )

    # Generate XML
    xml_element = restore_file.to_element()
    xml_str = etree.tostring(xml_element, pretty_print=True, encoding="unicode")

    # Expected XML string
    expected_xml = textwrap.dedent(
        """\
    <file name="example.txt" size="1024" mtime="1633036800" md5="d41d8cd98f00b204e9800998ecf8427e">
      <bd>backup2</bd>
      <bd>backup1</bd>
    </file>
    """
    )

    # Parse the generated and expected XML strings
    generated_element = etree.fromstring(xml_str)
    expected_element = etree.fromstring(expected_xml)

    # Extract the <bd> elements from both generated and expected XML
    generated_bds = {bd.text for bd in generated_element.findall("bd")}
    expected_bds = {bd.text for bd in expected_element.findall("bd")}

    # Compare the sets of <bd> elements
    assert generated_bds == expected_bds

    # Compare other attributes
    assert generated_element.attrib == expected_element.attrib


def test_restore_directory_to_xml():
    # Create a RestoreDirectory instance
    restore_directory = RestoreDirectory(name="example_dir")

    # Add files to the directory
    restore_file1 = RestoreFile(
        name="file1.txt",
        size=2048,
        file_path=None,
        mtime=1633036801,
        md5="d41d8cd98f00b204e9800998ecf8427e",
        bkp_dests={"backup1"},
    )
    restore_file2 = RestoreFile(
        name="file2.txt",
        size=4096,
        file_path=None,
        mtime=1633036802,
        md5="d41d8cd98f00b204e9800998ecf8427f",
        bkp_dests={"backup2"},
    )
    restore_directory.add_file(restore_file1)
    restore_directory.add_file(restore_file2)

    # Generate XML
    xml_element = restore_directory.to_element()
    xml_str = etree.tostring(xml_element, pretty_print=True, encoding="unicode")

    # Expected XML string
    expected_xml = textwrap.dedent(
        """\
        <dr name="example_dir">
          <file name="file1.txt" size="2048" mtime="1633036801" md5="d41d8cd98f00b204e9800998ecf8427e">
            <bd>backup1</bd>
          </file>
          <file name="file2.txt" size="4096" mtime="1633036802" md5="d41d8cd98f00b204e9800998ecf8427f">
            <bd>backup2</bd>
          </file>
        </dr>
        """
    )

    # Compare the generated XML with the expected XML
    assert xml_str.strip() == expected_xml.strip()


@pytest.mark.asyncio
async def test_restore_context_to_xml():
    # Create a RestoreContext instance
    restore_context = RestoreContext(bdsa=None)

    # Manually build the file structure
    restore_directory = RestoreDirectory(name="src1")
    sub_directory = RestoreDirectory(name="dir1")

    restore_file1 = RestoreFile(
        name="file1.txt",
        size=1024,
        file_path=None,
        mtime=1633036800,
        md5="d41d8cd98f00b204e9800998ecf8427e",
        bkp_dests={"backup1"},
    )
    restore_file2 = RestoreFile(
        name="file2.txt",
        size=2048,
        file_path=None,
        mtime=1633036801,
        md5="d41d8cd98f00b204e9800998ecf8427f",
        bkp_dests={"backup2"},
    )

    sub_directory.add_file(restore_file1)
    sub_directory.add_file(restore_file2)
    restore_directory.add_subdirectory(sub_directory)
    restore_context.file_structure["src1"] = restore_directory

    # Generate XML
    xml_element = restore_context.to_element()
    xml_str = etree.tostring(xml_element, pretty_print=True, encoding="unicode")

    # Expected XML string
    expected_xml = textwrap.dedent(
        """\
  <root>
    <rc src_path="src1">
      <dr name="dir1">
        <file name="file1.txt" size="1024" mtime="1633036800" md5="d41d8cd98f00b204e9800998ecf8427e">
          <bd>backup1</bd>
        </file>
        <file name="file2.txt" size="2048" mtime="1633036801" md5="d41d8cd98f00b204e9800998ecf8427f">
          <bd>backup2</bd>
        </file>
      </dr>
    </rc>
  </root>
  """
    )

    # Compare the generated XML with the expected XML
    assert xml_str.strip() == expected_xml.strip()


def test_restore_file_from_xml():
    # XML string for a RestoreFile
    xml_str = textwrap.dedent(
        """\
        <file name="example.txt" size="1024" mtime="1633036800" md5="d41d8cd98f00b204e9800998ecf8427e">
          <bd>backup1</bd>
          <bd>backup2</bd>
        </file>
        """
    )
    # Parse XML string to an etree.Element
    element = etree.fromstring(xml_str)

    # Create RestoreFile instance from XML
    restore_file = RestoreFile.from_element(element)

    # Expected RestoreFile instance
    expected_restore_file = RestoreFile(
        name="example.txt",
        size=1024,
        file_path=None,
        mtime=1633036800,
        md5="d41d8cd98f00b204e9800998ecf8427e",
        bkp_dests={"backup1", "backup2"},
    )

    # Compare the created instance with the expected instance
    assert restore_file == expected_restore_file


def test_restore_directory_from_xml():
    # XML string for a RestoreDirectory
    xml_str = textwrap.dedent(
        """\
        <dr name="example_dir">
          <file name="file1.txt" size="2048" mtime="1633036801" md5="d41d8cd98f00b204e9800998ecf8427e">
            <bd>backup1</bd>
          </file>
          <file name="file2.txt" size="4096" mtime="1633036802" md5="d41d8cd98f00b204e9800998ecf8427f">
            <bd>backup2</bd>
          </file>
        </dr>
        """
    )
    # Parse XML string to an etree.Element
    element = etree.fromstring(xml_str)

    # Create RestoreDirectory instance from XML
    restore_directory = RestoreDirectory.from_element(element)

    # Expected RestoreDirectory instance
    expected_restore_directory = RestoreDirectory(name="example_dir")
    expected_restore_directory.add_file(
        RestoreFile(
            name="file1.txt",
            size=2048,
            file_path=None,
            mtime=1633036801,
            md5="d41d8cd98f00b204e9800998ecf8427e",
            bkp_dests={"backup1"},
        )
    )
    expected_restore_directory.add_file(
        RestoreFile(
            name="file2.txt",
            size=4096,
            file_path=None,
            mtime=1633036802,
            md5="d41d8cd98f00b204e9800998ecf8427f",
            bkp_dests={"backup2"},
        )
    )

    # Compare the created instance with the expected instance
    assert restore_directory == expected_restore_directory


@pytest.mark.asyncio
async def test_restore_context_from_xml():
    # XML string for a RestoreContext
    xml_str = textwrap.dedent(
        """\
        <root>
          <rc src_path="src1">
            <dr name="dir1">
              <file name="file1.txt" size="1024" mtime="1633036800" md5="d41d8cd98f00b204e9800998ecf8427e">
                <bd>backup1</bd>
              </file>
              <file name="file2.txt" size="2048" mtime="1633036801" md5="d41d8cd98f00b204e9800998ecf8427f">
                <bd>backup2</bd>
              </file>
            </dr>
          </rc>
        </root>
        """
    )
    # Parse XML string to an etree.Element
    element = etree.fromstring(xml_str)

    # Create RestoreContext instance from XML
    restore_context = RestoreContext.from_element(element)

    # Expected RestoreContext instance
    expected_restore_context = RestoreContext(bdsa=None)
    sub_directory = RestoreDirectory(name="dir1")
    sub_directory.add_file(
        RestoreFile(
            name="file1.txt",
            size=1024,
            file_path=None,
            mtime=1633036800,
            md5="d41d8cd98f00b204e9800998ecf8427e",
            bkp_dests={"backup1"},
        )
    )
    sub_directory.add_file(
        RestoreFile(
            name="file2.txt",
            size=2048,
            file_path=None,
            mtime=1633036801,
            md5="d41d8cd98f00b204e9800998ecf8427f",
            bkp_dests={"backup2"},
        )
    )
    src_directory = RestoreDirectory(name="src1")
    src_directory.add_subdirectory(sub_directory)
    expected_restore_context.file_structure["src1"] = src_directory

    # Compare the created instance with the expected instance
    assert restore_context.file_structure == expected_restore_context.file_structure
