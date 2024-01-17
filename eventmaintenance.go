package ehscylla

import (
	"context"

	"github.com/scylladb/gocqlx/v2/qb"

	eh "github.com/looplab/eventhorizon"
)

// Replace implements the Replace method of the eventhorizon.EventStore interface.
func (s *EventStore) Replace(ctx context.Context, event eh.Event) error {
	e := &AggregateEvent{
		AggregateID: event.AggregateID(),
		Version:     event.Version(),
	}

	var num int
	err := qb.Select(EventTable.Name()).CountAll().Where(qb.EqLit("aggregate_id", event.AggregateID().String())).Query(s.db).Scan(&num)

	if err != nil {
		return &eh.EventStoreError{
			Err:         err,
			Op:          eh.EventStoreOpLoad,
			AggregateID: event.AggregateID(),
		}
	}

	if num == 0 {
		return &eh.EventStoreError{
			Err:         eh.ErrAggregateNotFound,
			Op:          eh.EventStoreOpLoad,
			AggregateID: event.AggregateID(),
		}
	}

	err = s.db.Query(EventTable.Get()).BindStruct(e).GetRelease(e)

	if err != nil {
		return &eh.EventStoreError{
			Err:         eh.ErrEventNotFound,
			AggregateID: event.AggregateID(),
		}
	}

	e, err = s.newDBEvent(ctx, event)

	if err != nil {
		return &eh.EventStoreError{
			Err:              err,
			Op:               eh.EventStoreOpReplace,
			AggregateID:      event.AggregateID(),
			AggregateType:    event.AggregateType(),
			AggregateVersion: event.Version(),
		}
	}

	updateColumns := []string{"event_type", "raw_event_data", "raw_meta_data", "aggregate_type", "timestamp", "event_id"}
	err = s.db.Query(EventTable.Update(updateColumns...)).BindStruct(&e).ExecRelease()

	if err != nil {
		return &eh.EventStoreError{
			Err:              err,
			Op:               eh.EventStoreOpReplace,
			AggregateID:      event.AggregateID(),
			AggregateType:    event.AggregateType(),
			AggregateVersion: event.Version(),
		}
	}

	return nil
}

// RenameEvent implements the RenameEvent method of the eventhorizon.EventStore interface.
func (s *EventStore) RenameEvent(ctx context.Context, from, to eh.EventType) error {
	var aggregate []*AggregateEvent

	err := s.db.Query(qb.Select(EventTable.Name()).Columns(EventTable.Metadata().Columns...).
		AllowFiltering().ToCql()).BindMap(qb.M{"event_type": from.String()}).SelectRelease(&aggregate)

	if err != nil {
		return &eh.EventStoreError{
			Err: eh.ErrEntityNotFound,
		}
	}

	for _, e := range aggregate {
		e.EventType = to
		err := s.db.Query(EventTable.Update("event_type")).BindStruct(&e).ExecRelease()

		if err != nil {
			return &eh.EventStoreError{
				Err: ErrCouldNotSaveAggregate,
				Op:  eh.EventStoreOpSave,
			}
		}
	}

	return nil
}

// Clear clears the event storage.
func (s *EventStore) Clear(ctx context.Context) error {
	err := s.db.ExecStmt("TRUNCATE TABLE event_store")

	if err != nil {
		return &eh.EventStoreError{
			Err: err,
		}
	}

	return nil
}
