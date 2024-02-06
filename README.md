# pgstream

A simple to use postgresql package built to read the postgresql replication log

### Features

- Reliable reading of the postgresql replication log (WAL log).
- Checkpoint support resume from previous point in case of failure or restart.
- Snapshotting support to get old data which doesn't exist in the WAL log. 