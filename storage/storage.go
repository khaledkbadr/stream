package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/khaledkbadr/stream/domain"

	_ "github.com/lib/pq"
)

type EventStore struct {
	*sqlx.DB
}

// NewEventStore connects to the database
func NewEventStore(driver, dataSourceName string) (*EventStore, error) {
	db, err := sqlx.Open(driver, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}

	return &EventStore{
		DB: db,
	}, nil
}

// InsertEvent creates event and insert it to the database
func (s *EventStore) InsertEvent(ctx context.Context, event domain.Event) error {
	eventFields, _ := event.ExtraFields.Value()
	_, err := s.ExecContext(ctx, "INSERT INTO events (id, type, time, extra_fields) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING", event.ID, event.Type, event.Time, eventFields)
	if err != nil {
		return fmt.Errorf("error create event: %w", err)
	}
	return nil
}

// GetEvent get event from database based on its event type and a time range
func (s *EventStore) GetEvent(ctx context.Context, eventType string, start time.Time, end time.Time) (*domain.Event, error) {
	var e domain.Event

	row := s.QueryRowxContext(ctx, `SELECT * FROM events WHERE type = $1 AND time BETWEEN $2 and $3 ORDER BY RANDOM() LIMIT 1`, eventType, start, end)
	if row.Err() != nil {
		return nil, fmt.Errorf("error getting event: %w", row.Err())
	}

	err := row.StructScan(&e)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("error scanning event: %w", err)
	}

	return &e, nil
}
