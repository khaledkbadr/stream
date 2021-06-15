# Database Reader Writer

## Implementation

### DB Schema
Flexibility of the event schemas is one of the main requirements for this application. `time` though is a field that will exist for all events and we need to query on a range of it. Also fast reads are essential.

Since PostgreSQL and most of the modern databases have JSON attribute type, I chose to use it for all the fields that can change while having `time` and `event_type` in their own columns in an `events` database.
Event's `ID` is hash of the event's data to avoid duplicate events

**Pros:**
* Felixible to add new fields to an event schema or change its type
* Support indexing on `time` and `event_type` to speed up the query
* No joins between multiple tables
* Old data is perserved out of the box

**Cons:**
* no type validation or schema validation on the event's variant fields.

## How to Run
* for the application to run you'll need a postgreSQL database running, you can spawn an ephemiral dockerized database using `make postgres` and `make migrate` to set up the `events` table
* there're a `make` commands already in the make file that will build and start writer and reader processes

```
make reader
make writer
```

## Testing
* I implemented some tests for storage layer which can be ran by running `make test`
* full integration test suite need to be implemented