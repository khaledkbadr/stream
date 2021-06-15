package domain

import (
	"crypto/md5"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ExtraFields map alias for event fields that change
type ExtraFields map[string]interface{}

type Event struct {
	ID          string      `json:"-" db:"id"`
	Type        string      `json:"type" db:"type"`
	Time        time.Time   `json:"time" db:"time"`
	ExtraFields ExtraFields `json:",inline" db:"extra_fields"`
}

func NewEvent(typ string, t time.Time, extraFields map[string]interface{}) Event {
	h := md5.New()
	h.Write([]byte(typ))
	binary.Write(h, binary.LittleEndian, t.Unix())
	fields, _ := json.Marshal(extraFields)
	h.Write(fields)
	md5sum := fmt.Sprintf("%x", h.Sum(nil))
	return Event{
		ID:          md5sum,
		Type:        typ,
		Time:        t,
		ExtraFields: extraFields,
	}
}

// Returns the JSON-encoded representation
func (e *ExtraFields) Value() (driver.Value, error) {
	// Marshal into json
	return json.Marshal(e)
}

// Decodes a JSON-encoded value
func (e *ExtraFields) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	// Unmarshal from json to map[string]float32
	x := make(map[string]interface{})
	if err := json.Unmarshal(b, &x); err != nil {
		return err
	}
	*e = x
	return nil
}

func (e *Event) UnmarshalJSON(data []byte) error {
	input := make(map[string]interface{})
	err := json.Unmarshal(data, &input)
	if err != nil {
		return err
	}

	t, err := time.Parse(time.RFC3339, input["time"].(string))
	if err != nil {
		return err
	}

	e.Time = t
	e.Type = input["type"].(string)
	e.ID = input["id"].(string)
	delete(input, "time")
	delete(input, "type")
	delete(input, "id")
	e.ExtraFields = input
	return nil
}

func (e *Event) MarshalJSON() ([]byte, error) {
	input := make(map[string]interface{})
	input["time"] = e.Time
	input["id"] = e.ID
	input["type"] = e.Type

	for k, v := range e.ExtraFields {
		input[k] = v
	}

	return json.Marshal(input)
}
