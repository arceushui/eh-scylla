package ehscylla

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/gocql/gocql"
	testutil "github.com/looplab/eventhorizon/eventstore"
	"github.com/looplab/eventhorizon/namespace"
	"github.com/scylladb/gocqlx/v2"
)

func TestEventStore(t *testing.T) {
	// Create gocql cluster.
	cluster := gocql.NewCluster(os.Getenv("SCYLLA_HOST"))
	cluster.Keyspace = os.Getenv("KEYSPACE")
	cluster.Timeout = 5 * time.Second
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	// Wrap session on creation, gocqlx session embeds gocql.Session pointer. 
	db, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store, err := NewEventStore(db)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}
	if store == nil {
		t.Fatal("there should be a store")
	}

	ctx := namespace.NewContext(context.Background(), "ns")

	// Run the actual test suite.
	t.Log("event store with default namespace")
	testutil.AcceptanceTest(t, store, context.Background())

	t.Log("event store with other namespace")
	testutil.AcceptanceTest(t, store, ctx)

	t.Log("event store with snapshot store")
	testutil.SnapshotAcceptanceTest(t, store, context.Background())
}