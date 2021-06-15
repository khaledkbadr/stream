package service

import (
	"context"
	"time"

	"github.com/khaledkbadr/stream/domain"
)

type Storage interface {
	InsertEvent(ctx context.Context, event domain.Event) error
	GetEvent(ctx context.Context, eventType string, start time.Time, end time.Time) (*domain.Event, error)
}
