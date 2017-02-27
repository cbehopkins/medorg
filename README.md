# medorg
Media Organiser - GUI for local file organisation

Basic GUI to tag and open files
Tags can be added to a file and then searches can be done to see which files match multiple tags

Checks for duplicate files by computing an MD5 for each file and comparing this to every other file. If filesize and MD5 checksum are the same, in all probability it is a duplicate file.

Each directory has a .medorg.xml file that contains or each file any checksums that have been computed and any tags that have been set.
Expeimented perviously(in another project) with multi-threaded md5 calculation found it was slower to do several at once as the filesystem worked best reading a single file at once and the md5 was not that compute intensive.

*Design Detail*

