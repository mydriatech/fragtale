# Database provider implementation for Apache Cassandra®

[Apache Cassandra®](https://cassandra.apache.org/) is highly scalable
distributed database.

## Is Cassandra really suitable for event-based systems?

For example [Cassandra Anti-Patterns: Queues and Queue-like Datasets](https://www.datastax.com/blog/cassandra-anti-patterns-queues-and-queue-datasets)
describes potential problems with using Cassandra as a message queue.

When events are sharded properly by row and kept for event sourcing and audit in Fragtale, performance will not degrade over time due to tombstones (deletes)
like in a classic message queue systems.

An alternative approach would be to use Kafka for events and feed these into Cassandra for queries and use self-contained protection of documents.
If such setup is already operational inside your organization, this project provides little value.

## Uniqueness with mininmal LWT operations

Cassandras Light Weight Transaction (LWT) mechanism works well for a low number
of concurrent operations, but introduces too much overhead for high performance
scenarios.

To generate unique cluster wide identifiers each Fragtale instance will leverage
LWTs during startup to claim an Instance Identifier (small integer) and bake
this into less significant bits of more complex identifers like "unique time".

## Mapping of logical entities

The core application will use a dedicated namespace for tables keep a global
application state.

Topics are mapped to dedicated keyspaces each with its own consumers, events
and integrity protection in separate tables.

## Indexing of event document content

Indexing of event documents is achieved by extracting values from the JSON
documents and adding these as additional columns in the `event` table.

The additional column is indexed usign Cassandra's StorageAttachedIndex.

## Sharding by time

Large collections of entities are shareded and ordered by time.

When lookup or iteration is needed, additional tables are added where shards can
be efficiently found and iterated over.

For very large datasets, two levels of lookup tables are used.
