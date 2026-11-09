# Glossary

## Backup inventory

The source and destination content considered during one backup run when deciding which files need to be copied.

## Restore context

The work of restoring a journal's files to their configured destinations, potentially across multiple backup volumes and `mdrestore` invocations. Its progress must remain available while that restore is in progress; it is not a permanent archive of completed restores.

## Backup destination

A volume that holds, or is intended to hold, a copy of backed-up content. A single backup may use multiple destinations to cover a larger source collection.
