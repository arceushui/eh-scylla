package ehscylla

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/looplab/eventhorizon/eventstore"
	"github.com/looplab/eventhorizon/namespace"
	"github.com/scylladb/gocqlx/v2"
)

func TestEventStoreMaintenanceIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

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

	defer store.Close()

	eventstore.MaintenanceAcceptanceTest(t, store, store, ctx)
}
