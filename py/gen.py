import sys
from pathlib import Path

from bkp_p.bkp_xml import BkpXml


def main():
    if len(sys.argv) > 1:
        directories = sys.argv[1:]
    else:
        directories = ["."]

    for directory in directories:
        directory = Path(directory)
        if directory.is_dir():
            BkpXml.update_path(directory)
            print(f".bkp.xml file created/updated for {directory}")
        else:
            print(f"Error: {directory} is not a valid directory.")


if __name__ == "__main__":
    main()
