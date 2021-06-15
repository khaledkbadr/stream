package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/khaledkbadr/stream/service"
	"github.com/khaledkbadr/stream/storage"
	"github.com/urfave/cli/v2"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("Teminated, error: %s\n", err)
		os.Exit(1)
	}
}

func run() error {
	app := cli.NewApp()
	app.Version = "0.0.1"
	app.Name = "Database Writer/Reader"
	app.Usage = "Simulate a pub sub stream application"

	var (
		schema         string
		dataSourceName string
		schemaMap      map[string]map[string]string
	)

	app.Flags = []cli.Flag{
		&cli.StringFlag{Name: "db-source-name", Usage: "db source name", Destination: &dataSourceName, Required: true},
		&cli.StringFlag{Name: "schema", Usage: "schema path", Destination: &schema, Required: true},
	}

	app.Before = func(c *cli.Context) error {
		// parsing schema to a map of objects
		// the map format allow us to generate events of the correct type to insert into the db
		rawSchema := make(map[string]map[string]interface{})
		data, err := ioutil.ReadFile(schema)
		if err != nil {
			return fmt.Errorf("failed to read schema: %w", err)
		}

		err = json.Unmarshal(data, &rawSchema)
		if err != nil {
			return fmt.Errorf("failed to parse schema: %w", err)
		}

		schemaMap = make(map[string]map[string]string)
		for key, value := range rawSchema {
			mappings := value["type_mapping"].(map[string]interface{})
			mappingsMap := make(map[string]string)
			for mappingKey, mappingValue := range mappings {
				mappingsMap[mappingKey] = mappingValue.(string)
			}
			schemaMap[key] = mappingsMap
		}

		return nil
	}

	app.Commands = []*cli.Command{
		{
			Name:  "writer",
			Usage: "start database writer",
			Action: func(c *cli.Context) error {
				// connecting to database
				store, err := storage.NewEventStore("postgres", dataSourceName)
				if err != nil {
					return err
				}

				defer store.Close()
				readerWriter := service.NewReaderWriter(store, schemaMap)
				readerWriter.StartWriter(c.Context, 5*time.Second)

				return nil
			},
		},
		{
			Name:  "reader",
			Usage: "start database reader",
			Action: func(c *cli.Context) error {
				// connecting to database
				store, err := storage.NewEventStore("postgres", dataSourceName)
				if err != nil {
					return err
				}
				defer store.Close()
				readerWriter := service.NewReaderWriter(store, schemaMap)
				readerWriter.StartReader(c.Context, 1*time.Second, 10)
				return nil
			},
		},
	}

	return app.Run(os.Args)
}
