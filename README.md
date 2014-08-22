mysqltools
==========

These tools were written at a time when my adrenaline levels were way way high.

syncdb.py copies missing rows between two databases based on missing primary keys

replicationsync.py repeatedly skips duplicate row replication errors to bring replication back in sync.

These are _NOT GENERALLY SAFE_, but can help automate recovery of _SOME_ types of cluster partitioning.

mysqlsnapshot.py is useful for creating snapshots to copy to remote replicas and supports both LVM and btrfs snapshots. This does not mean I think it's wise to use btrfs under your MySQL production cluster.
