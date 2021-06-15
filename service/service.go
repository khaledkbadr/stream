package service

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"log"

	"github.com/khaledkbadr/stream/domain"
)

// ReaderWriter wrapper for Writer and Reader structs
type ReaderWriter struct {
	Writer
	Reader
}

func NewReaderWriter(store Storage, schema map[string]map[string]string) *ReaderWriter {
	return &ReaderWriter{
		Writer: NewWriter(store, schema),
		Reader: NewReader(store, schema),
	}
}

// Writer handles writing capabilities to the database
type Writer struct {
	store      Storage
	schema     map[string]map[string]string
	eventTypes []string
}

func NewWriter(store Storage, schema map[string]map[string]string) Writer {
	eventTypes := make([]string, 0, len(schema))
	for key := range schema {
		eventTypes = append(eventTypes, key)
	}

	return Writer{
		store:      store,
		schema:     schema,
		eventTypes: eventTypes,
	}
}

// StartWriter writes a random event on time interval
func (w *Writer) StartWriter(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			eventType := w.eventTypes[rand.Intn(len(w.eventTypes))]

			// generate random values based on the event schema
			extraFields := make(map[string]interface{})
			for field, fieldType := range w.schema[eventType] {
				if field == "time" {
					continue
				}
				// only supports bigint and int types at the moemnt
				switch fieldType {
				case "bigint":
					extraFields[field] = rand.Intn(int(^uint32(0) >> 1))
				case "int":
					extraFields[field] = rand.Intn(int(^uint64(0) >> 1))
				}
			}

			event := domain.NewEvent(eventType, time.Now().UTC(), extraFields)
			err := w.store.InsertEvent(ctx, event)
			if err != nil {
				// this will log the error but not crash the application
				fmt.Println("failed to create event: ", err)
			}
		}
	}
}

// Reader handles reading capabilities to the database
type Reader struct {
	store         Storage
	schema        map[string]map[string]string
	eventTypes    []string
	workerChannel chan struct{}
}

func NewReader(store Storage, schema map[string]map[string]string) Reader {
	eventTypes := make([]string, 0, len(schema))
	for key := range schema {
		eventTypes = append(eventTypes, key)
	}

	return Reader{
		store:         store,
		schema:        schema,
		eventTypes:    eventTypes,
		workerChannel: make(chan struct{}),
	}
}

// worker reads a random event from store on a channel input
func (r *Reader) worker(ctx context.Context) {
	for range r.workerChannel {
		eventType := r.eventTypes[rand.Intn(len(r.eventTypes))]

		// generate random start tiem and end time within the past 5 minutes
		startTime := time.Now().UTC().Add(time.Duration(-rand.Intn(300)) * time.Second)
		endTime := startTime.Add(time.Duration(rand.Intn(300)) * time.Second)

		event, err := r.store.GetEvent(ctx, eventType, startTime, endTime)
		if err != nil {
			// this will log the error but not crash the application
			fmt.Println("failed to get event: ", err)
			continue
		}

		if event == nil {
			log.Printf("no event found for %s between %s and %s\n", eventType, startTime, endTime)
			continue
		}

		eventJSON, err := event.MarshalJSON()
		if err != nil {
			log.Printf("failed to parse event: %s\n", err)
			continue
		}

		log.Print("Reading event: ", string(eventJSON))
	}
}

// StartReader initialize a worker pool that reads from store on interval
// every worker reads 1 event from store on interval
func (r *Reader) StartReader(ctx context.Context, interval time.Duration, numWorkers int) error {
	ticker := time.NewTicker(1 * time.Second)

	for i := 0; i < numWorkers; i++ {
		go r.worker(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			close(r.workerChannel)
			return nil
		case <-ticker.C:
			for i := 0; i < numWorkers; i++ {
				r.workerChannel <- struct{}{}
			}
		}
	}
}
