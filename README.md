# EventHorizon with ScyllaDB in Golang

## Overview

This is an implementation of eventhorizon.EventStore for ScyllaDB.

## Installation

```bash
go get ./...
```

## Usage

- EventStore

```golang
cluster := gocql.NewCluster(os.Getenv("SCYLLA_HOST"))
cluster.Keyspace = os.Getenv("KEYSPACE")
cluster.Timeout = 5 * time.Second
cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

db, err := gocqlx.WrapSession(cluster.CreateSession())
// Create the event store.
store, err := ehscylla.NewEventStore(db)
```
