package ehscylla

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/namespace"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/gocqlx/v2/table"
)

// ErrCouldNotCloseDB is when the database could not be close.
var ErrCouldNotCloseDB = errors.New("could not close database session")

// ErrConflictVersion is when a version conflict occurs when saving an aggregate.
var ErrVersionConflict = errors.New("can not create/update aggregate")

// ErrCouldNotMarshalEvent is when an event could not be marshaled into JSON.
var ErrCouldNotMarshalEvent = errors.New("could not marshal event")

// ErrCouldNotUnmarshalEvent is when an event could not be unmarshalled into a concrete type.
var ErrCouldNotUnmarshalEvent = errors.New("could not unmarshal event")

// ErrCouldNotSaveAggregate is when an aggregate could not be saved.
var ErrCouldNotSaveAggregate = errors.New("could not save aggregate")

// EventStore implements an eh.EventStore for PostgreSQL.
type EventStore struct {
	db      gocqlx.Session
	encoder Encoder
}

var _ = eh.EventStore(&EventStore{})

type AggregateEvent struct {
	EventID       [16]byte        `db:"event_id"`
	Namespace     string           `db:"namespace"`
	AggregateID   [16]byte        `db:"aggregate_id"`
	AggregateType eh.AggregateType `db:"aggregate_type"`
	EventType     eh.EventType     `db:"event_type"`
	RawEventData  json.RawMessage  `db:"raw_event_data"`
	Timestamp     time.Time        `db:"timestamp"`
	Version       int              `db:"version"`
	metaData      map[string]interface{}
	data        eh.EventData
	RawMetaData json.RawMessage `db:"raw_meta_data"`
}

var EventTable = table.New(table.Metadata{
	Name: "event_store",
	Columns: []string{
		"event_id",
		"namespace",
		"aggregate_id",
		"aggregate_type",
		"event_type",
		"raw_event_data",
		"timestamp",
		"version",
		"raw_meta_data",
	},
	PartKey: []string{
		"aggregate_id",
	},
	SortKey: []string{
		"version",
	},
})

func (a AggregateEvent) MarshalBinary() (data []byte, err error) {
	return json.Marshal(a)
}

func (a *AggregateEvent) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, a)
}

// NewUUID for mocking in tests
var NewUUID = uuid.New

// newDBEvent returns a new dbEvent for an event.
func (s *EventStore) newDBEvent(ctx context.Context, event eh.Event) (*AggregateEvent, error) {
	ns := namespace.FromContext(ctx)

	// Marshal event data if there is any.
	rawEventData, err := s.encoder.Marshal(event.Data())
	if err != nil {
		return nil, &eh.EventStoreError{
			Err: ErrCouldNotMarshalEvent,
		}
	}

	// Marshal meta data if there is any.
	rawMetaData, err := json.Marshal(event.Metadata())
	if err != nil {
		return nil, &eh.EventStoreError{
			Err: ErrCouldNotMarshalEvent,
		}
	}

	return &AggregateEvent{
		EventID:       NewUUID(),
		AggregateID:   event.AggregateID(),
		AggregateType: event.AggregateType(),
		EventType:     event.EventType(),
		Version:       event.Version(),
		Timestamp:     event.Timestamp(),
		Namespace:     ns,
		RawEventData:  rawEventData,
		RawMetaData:   rawMetaData,
	}, nil
}

// NewEventStore creates a new EventStore.
func NewEventStore(db gocqlx.Session) (*EventStore, error) {
	s := &EventStore{
		db:      db,
		encoder: &jsonEncoder{},
	}

	err := s.db.ExecStmt(`CREATE TABLE IF NOT EXISTS event_store (
				event_id uuid,
				namespace text,
				aggregate_id uuid,
				aggregate_type text,
				event_type text,
				raw_event_data blob,
				timestamp timestamp,
				version int,
				raw_meta_data blob,
				PRIMARY KEY (aggregate_id, version)
			) WITH CLUSTERING ORDER BY (version DESC)`)

	if err != nil {
		return nil, err
	}

	return s, nil
}

// Save implements the Save method of the eventhorizon.EventStore interface.
func (s *EventStore) Save(ctx context.Context, events []eh.Event, originalVersion int) error {
	if len(events) == 0 {
		return &eh.EventStoreError{
			Err: eh.ErrMissingEvents,
			Op:  eh.EventStoreOpSave,
		}
	}

	dbEvents := make([]*AggregateEvent, len(events))
	id := events[0].AggregateID()
	at := events[0].AggregateType()

	// Build all event records, with incrementing versions starting from the
	// original aggregate version.
	for i, event := range events {
		// Only accept events belonging to the same aggregate.
		if event.AggregateID() != id {
			return &eh.EventStoreError{
				Err:              eh.ErrMismatchedEventAggregateIDs,
				Op:               eh.EventStoreOpSave,
				AggregateType:    at,
				AggregateID:      id,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}

		if event.AggregateType() != at {
			return &eh.EventStoreError{
				Err:              eh.ErrMismatchedEventAggregateTypes,
				Op:               eh.EventStoreOpSave,
				AggregateType:    at,
				AggregateID:      id,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}

		// Only accept events that apply to the correct aggregate version.
		if event.Version() != originalVersion+i+1 {
			return &eh.EventStoreError{
				Err:              eh.ErrIncorrectEventVersion,
				Op:               eh.EventStoreOpSave,
				AggregateType:    at,
				AggregateID:      id,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}

		// Create the event record for the DB.
		e, err := s.newDBEvent(ctx, event)
		if err != nil {
			return err
		}

		dbEvents[i] = e
	}

	for _, e := range dbEvents {
		q := s.db.Query(EventTable.Insert()).BindStruct(e)
		if err := q.ExecRelease(); err != nil {
			return &eh.EventStoreError{
				Err:              ErrCouldNotSaveAggregate,
				Op:               eh.EventStoreOpSave,
				AggregateType:    at,
				AggregateID:      id,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}
	}

	return nil
}

// Load implements the Load method of the eventhorizon.EventStore interface.
func (s *EventStore) Load(ctx context.Context, id uuid.UUID) ([]eh.Event, error) {
	return s.LoadFrom(ctx, id, 1)
}

// LoadFrom loads all events from version for the aggregate id from the store.
func (s *EventStore) LoadFrom(ctx context.Context, id uuid.UUID, version int) ([]eh.Event, error) {
	var aggregate []AggregateEvent

	idBytes, err := id.MarshalBinary()
	if err != nil {
		return nil, &eh.EventStoreError{
			Err:         err,
			Op:          eh.EventStoreOpLoad,
			AggregateID: id,
		}
	}

	err = s.db.Query(EventTable.SelectBuilder(EventTable.Metadata().Columns...).
		Where(qb.GtOrEqLit("version", strconv.FormatInt(int64(version), 10))).OrderBy("version", qb.ASC).ToCql()).
		BindMap(qb.M{"aggregate_id": idBytes}).
		SelectRelease(&aggregate)

	if err != nil {
		return nil, &eh.EventStoreError{
			Err:         fmt.Errorf("could not find event: %w", err),
			Op:          eh.EventStoreOpLoad,
			AggregateID: id,
		}
	}

	if len(aggregate) == 0 {
		return nil, &eh.EventStoreError{
			Err:         eh.ErrAggregateNotFound,
			Op:          eh.EventStoreOpLoad,
			AggregateID: id,
		}
	}

	var events []eh.Event

	for _, e := range aggregate {
		if e.EventType == "" {
			return nil, &eh.EventStoreError{
				Err:         fmt.Errorf("could not unmarshal event: %w", ErrCouldNotUnmarshalEvent),
				Op:          eh.EventStoreOpLoad,
				AggregateID: id,
			}
		}

		if e.data, err = s.encoder.Unmarshal(e.EventType, e.RawEventData); err != nil {
			return nil, &eh.EventStoreError{
				Err: ErrCouldNotUnmarshalEvent,
			}
		}
		e.RawEventData = nil

		if err = json.Unmarshal(e.RawMetaData, &e.metaData); err != nil {
			return nil, &eh.EventStoreError{
				Err: ErrCouldNotUnmarshalEvent,
			}
		}
		e.RawMetaData = nil

		event := eh.NewEvent(
			e.EventType,
			e.data,
			e.Timestamp,
			eh.ForAggregate(
				e.AggregateType,
				e.AggregateID,
				e.Version,
			),
			eh.WithMetadata(
				e.metaData,
			),
		)
		events = append(events, event)
	}

	return events, nil
}

func (s *EventStore) Close() error {
	if s.db.Session == nil {
		return ErrCouldNotCloseDB
	}
	s.db.Close()
	return nil
}
