# Introduction

This is the python version of the medorg toolset
Or at least the parts of it that are implemented in python.
In general that means the stuff that is more about the algorithm, and less about performance
It should work as a standalone tool, with the go versions being for performance in some tasks.

# Concepts

The core of this design is to be resilient to failures, it has to withstand and be recoverable from file corruption and even programming errors.

## Backup Database

There is a database that we create as part of the backup (the session file). This is intended to be treated as transient/ephemeral. It's useful to have it as a context, but it's not to be relied upon for backups - there's other ways to do this.

The backup database contains information read from the medback files.
To clarify, you can use the session file as your restore context. But That is not human editable, and the format of the database might change between versions. The restore context is much easier to deal with when problems occur.

## .medback files

The Trusted Source of truth is the .medback.xml files. There is one created in every directory. They contain the file specific information in an xml format. They are intended to be (mostly) human readable.

A file has an md5 hash and a file size. If two files have the same hash and size, we consider them the same files. (Checks do exist to prove this but this is a key assumption in the backup/restore process)

You end up with these files sprinked throughout your directory tree. If this ois a problem for you, this is not the tool for you.

## Backup Source

The source of a backup, a directory you want to be backed up.
You will probably have several directories you want to specify separately as backup sources

## Backup Target

The destination for backup data. You may have many of these. They may be smaller in capacity than the source. The Target have a file written to it with a unique identity information.

## Restore Context
This is an xml file that represents the directory structure of the backed up system. The XML structure represents the directory/file structure on disk. For each file there is a name, size and md5 hash. Upon restore the file will be copied from the target that matches the size and hash. It may take multiple restore commands from multiple targets to find/populate the missing files.

# Installation

Create a venv, install the package, whatever works for you...
(More detail here for newbies...)

# Process

To run a backup please first decide where you want your (ephemeral) backup database to live. It will default to \~/.bkup\_dest I have assumed in these instructions you will use the default, all commands accept --session-db {path} if you wish to specify a different path to the default.

Next you many wish to use check\_calc from the golang tooling - this will rapidly generate the md5 hashes for the directories.
Or just add the directory with
`medback add-src --srd-dir {target directory}`

This will walk the directory, create any medback files and hashes as needed and add the information to the database.
Do this for as many source directories as you require.

If at any point in the future you have files that change and you want to update the database from your source(s) then you can update:
`medback update`
And this will rescan all the source directories it is aware of.

Having done this you now have a database of files that need backing up. Time to write some data to a target device:
`medback target {path}`
This will copy the "best" files to the target until the target is full, or all files in the database have been copied to the target. The better a file is, the sooner it is copied.

The best files to copy are:
* The files that have been copied to the fewest targets previously
* The largest files

Having copied files to a target, the database (and the medback files) will be updated to indicate which files have been copied. The next time target is run, the best files algorithm will take this into account.

You may however from previous backups (e.g. rsync) already copied some files in your backup comntext to the target. If you expect that some of the files you had backed up will already be on the target, you can run:
`medback discover {path}`

This will initialise the path as a target (giving it a VolumeId) and search through the target to look at the md5 hashes and checksums it finds there. Where it finds files that match the database, the ddatabase will be updated to mark those files as being backed up to that target. This saves having to scrub old backups and allows this tool to co-exist with other backup solutions.

TBD Add backup stats command!
That shows which files have not been backed up yet

Having backed up you files you can now create a restore context. This will save the information in your database into a format that can be later read to re-populate the database. 
`medback write_restore {path}`
Likewise to populate the database from a restore file you can:
`medback add_restore_context {path}`
TBD add path rewrite to change paths - for now just edit the restore context


When you want to restore you backups there is the restore command
`medback restore {target}`
This will read the target and any files on the target that match the hash and size will be copied to the source with the filename and path specified in the restore context.

TBD add backup restore command
That shows which files have not been restored, which is the best next target to use.