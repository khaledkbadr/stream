package storage

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/khaledkbadr/stream/domain"
	_ "github.com/mattn/go-sqlite3"

	migrate "github.com/golang-migrate/migrate/v4"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randDBName(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func setUpDatabase() (string, error) {
	dbName := randDBName(10) + ".db"

	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		return "", err
	}

	defer func() {
		if err := db.Close(); err != nil {
			return
		}
	}()

	driver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	if err != nil {
		return "", err
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://../migrations",
		"ql", driver)
	if err != nil {
		log.Fatal(err)
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		log.Fatal("failed to execute migrations: ", err)
	}

	return dbName, nil
}

func tearDownDatabase(dbName string) error {
	return os.Remove(dbName)
}

func TestInsertEvent(t *testing.T) {
	dbName, err := setUpDatabase()
	if err != nil {
		t.Fatalf("failed to initialize database: %s", dbName)
	}

	defer tearDownDatabase(dbName)

	store, err := NewEventStore("sqlite3", dbName)
	if err != nil {
		t.Fatalf("failed to create event store for db: %s", dbName)
	}

	event := domain.NewEvent("test_event", time.Now().UTC(), nil)
	err = store.InsertEvent(context.Background(), event)
	if err != nil {
		t.Fatal("failed to insert event")
	}

	eventResult, err := store.GetEvent(context.Background(), "test_event", time.Now().UTC().Add(-1*time.Second), time.Now().UTC())
	if err != nil {
		t.Fatal("failed to get event")
	}

	if eventResult.ID != event.ID {
		t.Error("insert event didn't succeed")
	}
}

func TestInsertEvent_DuplicateEventSequential(t *testing.T) {
	dbName, err := setUpDatabase()
	if err != nil {
		t.Fatalf("failed to initialize database: %s", dbName)
	}

	defer tearDownDatabase(dbName)

	store, err := NewEventStore("sqlite3", dbName)
	if err != nil {
		t.Fatalf("failed to create event store for db: %s", dbName)
	}

	event := domain.NewEvent("test_event", time.Now().UTC(), nil)

	// insert event for first time
	err = store.InsertEvent(context.Background(), event)
	if err != nil {
		t.Fatal("failed to insert event")
	}

	// insert same event a second time
	err = store.InsertEvent(context.Background(), event)
	if err != nil {
		t.Fatal("failed to insert event")
	}

	rows := store.QueryRowx("SELECT COUNT(*) FROM events WHERE type = $1 AND time BETWEEN $2 and $3", "test_event", time.Now().UTC().Add(-2*time.Second), time.Now().UTC())
	if rows.Err() != nil {
		t.Fatal("failed to count events")
	}

	var count int
	if err := rows.Scan(&count); err != nil {
		log.Fatal(err)
	}

	if count != 1 {
		t.Errorf("event count is incorrect. expected: %d; found %d", 1, count)
	}
}

func TestInsertEvent_DuplicateEventConcurrent(t *testing.T) {
	dbName, err := setUpDatabase()
	if err != nil {
		t.Fatalf("failed to initialize database: %s", dbName)
	}

	defer tearDownDatabase(dbName)

	store, err := NewEventStore("sqlite3", dbName)
	if err != nil {
		t.Fatalf("failed to create event store for db: %s", dbName)
	}

	event := domain.NewEvent("test_event", time.Now().UTC(), nil)

	wg := sync.WaitGroup{}

	insert := func(wg *sync.WaitGroup) {
		defer wg.Done()
		// insert event for first time
		err := store.InsertEvent(context.Background(), event)
		if err != nil {
			t.Fatal("failed to insert event")
		}
	}

	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go insert(&wg)
	}

	wg.Wait()

	rows := store.QueryRowx("SELECT COUNT(*) FROM events WHERE type = $1 AND time BETWEEN $2 and $3", "test_event", time.Now().UTC().Add(-2*time.Second), time.Now().UTC())
	if rows.Err() != nil {
		t.Fatal("failed to count events")
	}

	var count int
	if err := rows.Scan(&count); err != nil {
		log.Fatal(err)
	}

	if count != 1 {
		t.Errorf("event count is incorrect. expected: %d; found %d", 1, count)
	}

}

func TestGetEvent_Random(t *testing.T) {
	dbName, err := setUpDatabase()
	if err != nil {
		t.Fatalf("failed to initialize database: %s", dbName)
	}

	defer tearDownDatabase(dbName)

	store, err := NewEventStore("sqlite3", dbName)
	if err != nil {
		t.Fatalf("failed to create event store for db: %s", dbName)
	}

	wg := sync.WaitGroup{}

	insert := func(time time.Time, wg *sync.WaitGroup) {
		defer wg.Done()
		event := domain.NewEvent("test_event", time, nil)
		// insert event for first time
		err := store.InsertEvent(context.Background(), event)
		if err != nil {
			t.Fatal("failed to insert event")
		}
	}

	// insert 10 different events with the same type
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go insert(time.Now().UTC().Add(-time.Duration(i)*time.Second), &wg)
	}

	wg.Wait()

	// read event 10 times and if some of the ids are different then it's random
	ids := make(map[string]struct{})
	for i := 1; i <= 10; i++ {

		eventResult, err := store.GetEvent(context.Background(), "test_event", time.Now().UTC().Add(-20*time.Second), time.Now().UTC())
		if err != nil {
			t.Fatal("failed to get event")
		}

		ids[eventResult.ID] = struct{}{}
	}

	if len(ids) <= 1 {
		t.Errorf("GetEvent doesn't return random events")
	}

}
