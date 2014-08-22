mysqldbsync
===========

Tools written at a terrible time

syncdb.py copies missing rows between two databases based on missing primary keys
replicationsync.py repeatedly skips duplicate row replication errors to bring replication back in sync.

These are _NOT GENERALLY SAFE_, but can help automate recovery of _SOME_ types of cluster partitioning.

There is also mysqlsnapshot.py which is useful for creating snapshots to copy to remote replicas and supports both LVM and btrfs snapshots. This does not mean I think it's wise to use btrfs under your MySQL production cluster.
