.PHONY:	 postgres migrate

postgres:
	docker run --rm --network host -e ALLOW_EMPTY_PASSWORD=yes bitnami/postgresql:11

migrate:
	migrate -source file://migrations \
			-database postgres://postgres@localhost/postgres?sslmode=disable up

build:  $(OUTPUT)
	CGO_ENABLED=0 GOOS=linux go build -o bin/app \
		-ldflags "-X main.version=$(VERSION)" \
		-gcflags "-trimpath $(GOPATH)/src"


reader: migrate build
	./bin/app --db-source-name "postgres://postgres@localhost/postgres?sslmode=disable" --schema "./schema.json" reader

writer: migrate build
	./bin/app --db-source-name "postgres://postgres@localhost/postgres?sslmode=disable" --schema "./schema.json" writer

test:
	@echo :: run tests
	go test ./... -v -race

$(OUTPUT):
	mkdir -p $(OUTPUT)