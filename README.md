---
title: "Media Organiser"
---
# Overview
A colleciton of utilities for helping with my media organisation.

The core mechanic is creating an md5 for each file. There is a record, per directory, that is created to store this and other information. The record is a file naled ".md5_list.xml". Per file we record its name, size, timestamp, and (md5) checksum. Other properties may be stored, these are discussed later as required.

There is a config file that may be located in the current directory, or found in the home directory ".medorg.xml"

# Utilities
## Check Calc
This utility performs the checksum calculation, calculating in the first place, and, where required, updating the checksum. It additionally can perform other useful features dicussed below.

A file's checksum is recalculated if its size or timestamp has changed from the file recorded in the record. 

While a filename is used to identify the file to the .md5_list.xml, it does not contribute to the other functions that rely on duplicate detection.

### Duplicate Detection and Deletion
I assume two files are the same if they have the same size and md5 checksum. It checks for duplicate files comparing the md5 to every other file. If filesize and MD5 checksum are the same, in all probability it is a duplicate file.

### Move Detection
Calculating the MD5 for large systems can be time consuming. We can optionally run a move detect pass. This will scan the full specified directory tree and collect a list of all files that are present in the record, and absent from the file system. It then looks for new files in the file system (files that are present, but do not hava an entry in the appropriate record.) If is finds a candidate with the same filename and size from its missing scan, then the checksum for that entry is used without re-calcualtion.

### AutoRename
It can be useful to auto-rename files according to certain rules. Where name collisions occur, then an auto numbering scheme is applied. There is a regular expression based syntax that can be specified by the main config file. Regular expresssion rules are applied after the extension and any auto-numbering is removed from the filename.

The regex relies on group matching. If there is a single group, then after the rename, the contents of the first group will apply. For example `^ (.*)$` will remove any leading spaces from file names, `^(.*)[ _-]$` wil remove training whitespace, underscores and hyphens, `^(.*)[|](.*)$` will remove pipe characters from within filenames.  etc.

### Concentrate
Move all files from subdirectories, into the current directory moving the record properties with them.

---

## Backup
Consider the situation where you have a larger file server than your backup medium. How does one backup a 1TB RAID, using 250GB medium. 

One can use the backup tool!

The backup tool takes the input as a source and destination directory. If none exists in the destination directory a .medorg.xml is created and a name assigned to the "volume". Both directory trees are then scanned as per the Check Calc tool to ensure their md5 checksums are accurate. Optionally a full recalculation can be forced.

Each of the source and destination trees are checked to see if either of the trees contain duplicates. A warning is issued in the case of any duplicates in the source tree. Duplicates on the destination are illegal, and the process errors out if any are found.

 Where a file exists in the source tree that has the same size and checksum as a file (of any name or directory location) in the destination tree, then these are taken to be a duplicate file. For these duplicate files, the source tree has its record for this file updated to include a reference to the volume name. Should the source tree contain duplicates, then only one is guaranteed to be updated with the volume information.

 With the source tree updated to include information on the files already on the destination disk, the source tree is again analysed. This is looking for files that can be copied to destination tree. A priority list of files is created to be copied to the destination list. The priorities used to calculate this are:

 * Number of volumes the file has been backed up to
 
     Being backed up to 0 volumes makes this file a top priority to be backed up.

     Being already backed up to this volume prevents the file from being copied regardless of other parameters

 * Size. Files are grossly gouped into size categories. Typically this would be set in the user's ".medorg.xml"

     Large files are copied first

