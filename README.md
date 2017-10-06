# cassandra_snapshot_tool
This tool is for Cassandra database backup and recovery at the keyspace level. It flushs the Cassandra memTable to SSTable, create the snapshot for the given keyspace and uploads the data onto s3 on the specified bucket. This tool also allow you to download the snapshots and copy it over the Cassandra ring(nodes) and refresh your keyspaces.
