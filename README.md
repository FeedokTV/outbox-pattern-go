# Outbox pattern in Go

Hello everyone! Thank you for choosing this repository to learn new patterns.
In this repository I want to show you two approaches to the outbox pattern.
All the code is written in Go, and as the database I use PostgreSQL (because it's the most common stack nowadays).

## The problem

A developer isn't just a person who writes code. A developer solves different kinds of problems and wires together things that are really hard to wire together.
Today we will talk about wiring two systems: a database and a message broker.

In many systems it's really necessary to make operations atomic. Junior developers often see a message broker as a system
where we can just drop a message after some action to notify other systems. **That's true, but** real systems usually don't work like that. We can't just "drop"
a message after, for example, something happened or was stored in the database.

Imagine a modern bank with nice online banking. Person A transfers money to Person B. Unfortunately, we can't just update the account balance,
because clients expect a lot of useful features. Let's break this process apart:

1) Person A sends money to Person B from the website on their PC
2) The website sends a request to our API (let's keep this step as simple as possible)
3) Our system receives an HTTP request about this transaction
4) We do some magic calculations and check that everything is okay with the transaction
5) We update the database

And here is the main point of the whole story: we need to tell a lot of services about this transaction:

6) Notification service (so Person B gets a notification on their phone)
7) Analytics service
8) Regulatory reporting (for example, reports on large cash transactions)
9) Anti-fraud AI

And so on. Of course, if the transaction was unsuccessful, we need to notify Person A and maybe some other services about it too.
A banking system is not just one UPDATE query in the database, so we need to make sure that
every component of our big system (for example, every microservice) is notified about every action clients take.
It's really necessary!

Let's take a look at a small example:

```go
_, err := db.Exec(ctx,
	`INSERT INTO transactions (id, sender, recipient, amount)
	 VALUES ($1, $2, $3, $4)`,
	tx.ID, tx.Sender, tx.Recipient, tx.Amount,
)

err = kafkaWriter.WriteMessages(ctx, kafka.Message{
	Key:   []byte(tx.ID),
	Value: payload,
})
```

What if _something_ happens between the SQL query and the Kafka write? Or Kafka is down at that moment, so the database receives the update but nobody else does?

We could swap these two operations and notify everybody before writing to the database. But the problem is still there: now other services may learn about a transaction that was never saved. So we need to make sure that the database operation and the message broker operation are atomic (or at least guarantee that nobody forgets about our operation).

## How the outbox solves this problem

This is where the outbox pattern comes in!
The name says it all: the main idea of the outbox pattern is to have a place where we store our notifications for other services, in the same database transaction as the data change. Then a separate process guarantees that everything that happened in those transactions is eventually delivered to the other parts of the system.

Let's return to our example: we need to guarantee that every transaction in the banking system produces an event for other systems.

Before we start: each solution has its own folder with the code and components related to it, but the core of our banking system is in the `internal` folder.
I strongly recommend taking a look there first.
As I said, I'm using Postgres for the database and Kafka as the message broker. They both run in containers via `docker compose`. The database schema is in `init-db.sql`.

## Approach 1 (Transactional outbox table)

The first approach I want to show is easy to understand and not that hard to implement.

For this approach we create a special table for the events that need to be delivered to Kafka.

Our imaginary banking system has a table for transactions:

```sql
CREATE TABLE IF NOT EXISTS transactions (
    id UUID PRIMARY KEY NOT NULL,
    sender VARCHAR(100) NOT NULL,
    recipient VARCHAR(100) NOT NULL,
    amount BIGINT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

For this approach we create another table where we store "events" in the same database transaction (not a banking one). Then a relay constantly picks up new records from the events table and sends them to Kafka. What if Kafka is down? That's why we mark records as sent or unsent: even if Kafka goes down for some reason, once it's back up, the relay selects only the unsent events and retries them.

So first, the table:

```sql
CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY NOT NULL,
    aggregate_id VARCHAR(100) NOT NULL, -- Kafka key
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL, -- full event envelope
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    locked_until TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- relay lease: row is free once this is in the past
    sent_at TIMESTAMPTZ -- NULL until the relay has published the event
);
```

You can also see the `locked_until` field. Once we "take" a record to process it, we need to "lock" it so another process doesn't steal it from us (for example, so a second relay instance doesn't send it at the same time).

Take a look at the `internal` folder to see how I implemented the helper functions for the transaction data model and the Postgres communication.

Then take a look at `solution_v1/internal/outbox/outbox.go`. Here I wrote helper functions
for our database actions. The `Emit` function creates a row in the outbox table.

To create a (banking) transaction we use the `CreateTransaction` function in `solution_v1/internal/service/transaction.go`. It creates two rows in two tables in one database transaction, so if something goes wrong, everything is rolled back.

But the most interesting part is the relay (`relay.go` in the `solution_v1` folder).

Here we have the `Relay` type, which has a store and a publisher. The store is the `OutboxStore` interface, which requires `Claim`, `MarkSent` and `Release` methods. The publisher is the `MessagePublisher` interface, which requires a `Publish` method. These abstractions keep the relay independent of any specific implementation, so we can easily use it with any database or message broker.

The main method of the relay is `Run`, which periodically (using a ticker and `pollInterval`) calls `processBatch`. This method claims the records that need to be sent and publishes them via `r.publisher.Publish`. After an event is published, we call `MarkSent`. Everything is simple, and that's why we made the abstraction!

For Kafka I wrote the `KafkaPublisher` type in `publisher/kafka.go` using sarama (a `SyncProducer`, just for the example).

For Postgres, take a look at the `PostgresStore` type in `outbox/store.go`. It implements all three methods we need, but the most interesting one, of course, is `Claim`. It selects from the outbox table all records that:
- are not locked (`locked_until` is in the past)
- were never sent (`sent_at` is NULL)

It also locks the selected rows with `FOR UPDATE SKIP LOCKED`, so a concurrent relay skips them instead of claiming the same batch, and then extends `locked_until` so the rows stay reserved for us after the claiming transaction commits.

Rows are claimed in `ORDER BY created_at, id` order, so events are published in the order they were created. If publishing fails, the relay stops at the failed event and releases the rest of the batch, so later events can't overtake it.

A known limitation: the lease (30 seconds) must be long enough for a whole batch (up to 100 events) to be published. If Kafka is slow and the lease expires mid-batch, another relay instance can claim the remaining rows, which leads to duplicates and reordering. With a single relay instance this can't happen. In production you would reduce the batch size or renew the lease before each publish.

Now that we have the `Relay` and implementations for its dependencies, we can wire everything together in `main.go`!

For our example we run two goroutines via `errgroup`. The first one runs the relay, and the second one runs a function that makes two banking transactions:

```go
	errGroup.Go(func() error {
		return outboxRelay.Run(groupCtx)
	})

	errGroup.Go(func() error {
		return bankingServiceExample(groupCtx, pool)
	})
```

Run it and see how it works!

## Approach 2 (CDC)

The second approach is more complex, but really interesting to implement.

When working with databases, there is a class of tools that lets us see every change in the database. This is called Change Data Capture (CDC): it captures every database change in real time and streams it to other systems or storage, so we don't have to transfer all the data at once.

So the main idea is to track changes in the database. In our example, that means tracking changes to the `transactions` table.

Okay, an interesting approach, but how do we do it with Postgres? Run a `SELECT` on the table every X seconds? Well... definitely not. But Postgres already maintains its own change log, the **Write-Ahead Log (WAL)**: a log (stored as a sequence of segment files) where every data modification is recorded before the changed data pages are written to disk. Only writes go to the WAL; reads like `SELECT` never appear there. Maybe we can read it continuously, pick out our changes, and send a message to Kafka for each of them? A nice idea!

So how do we do it properly? Thanks to Postgres replication. Postgres has different kinds of replication, and one of them is **logical replication**. **Physical** replication ships the WAL itself and replays it block-by-block, producing an exact copy of the whole database cluster. **Logical** replication decodes the WAL into row-level changes (which rows were inserted, updated or deleted), grouped by transaction, and can be limited to specific tables.

Note: WAL contains enough information for logical decoding only when the server runs with `wal_level = logical` (the default is `replica`). In this repository it is set in `docker-compose.yml`; changing it requires a server restart.

To get this stream of changes, we use a **replication slot**. A slot is a persistent server-side object: it survives disconnects and server restarts, and it remembers the last WAL position the consumer has confirmed. Postgres keeps all WAL after that position until the consumer confirms it. This is also the main danger of slots: if a consumer stops and its slot is left behind, WAL accumulates until the disk is full. Unused slots must be dropped (`pg_drop_replication_slot`), and in production you should cap the retained WAL with `max_slot_wal_keep_size`.

We also need to tell Postgres what we want to stream. For this we create a **publication**:

`Logical replication uses a publish and subscribe model with one or more subscribers subscribing to one or more publications on a publisher node. Subscribers pull data from the publications they subscribe to and may subsequently re-publish data to allow cascading replication or more complex configurations.` - as the documentation says.

So basically, a publication is a set of specific tables (or entire schemas) whose data changes are packaged for broadcast.

To create one, we can do something like:

```sql
CREATE PUBLICATION transactions_cdc;
```

The `pgoutput` plugin requires a publication name when we start replication. But in this repository we don't stream table rows: inside the same database transaction as the `INSERT`, we write the event into the WAL with `pg_logical_emit_message(true, '<prefix>', payload)`. Because the first argument is `true`, the message is transactional: it is decoded only if the transaction commits, and it arrives between that transaction's BEGIN and COMMIT. Logical messages are not filtered by publications, so the publication above can stay empty; instead, we must start replication with the `messages 'true'` plugin option, otherwise `pgoutput` doesn't send them at all.

Now, before coding, let's summarize what we have:

- WAL - the log where Postgres records every change before writing data pages to disk
- Replication slot - a persistent server-side object that remembers our confirmed position in the WAL (the LSN, Log Sequence Number), so Postgres doesn't delete WAL we haven't processed yet
- Publication - a set of tables whose changes are broadcast

Let's think about what we need to do:

1) Write a new relay that consumes the WAL stream
2) Write a Postgres implementation of our future abstraction that reads changes from the WAL, parses them and moves the LSN
3) Write a Kafka implementation for the new relay

Our event processing workflow will look like this:
1) Receive a message about new changes in the database
2) Parse the message and build a new one for the message broker
3) Make sure the message was sent successfully
4) Move the LSN to the end of the current transaction (so Postgres knows which data we have already read and can delete)

Sounds really easy, but unfortunately there are a lot of pitfalls, which we will describe later.

Let's begin with the relay. Take a look at `solution_v2/internal/relay/relay.go`.

Our `Relay` type has a `CDCSource` and a `MessagePublisher`. The new one is `CDCSource`: the source from which we receive database changes via the `Next()` method.

In `Relay.Run()` we run an infinite loop where we receive new events from `CDCSource.Next()`, of this type:

```go
type CDCEvent struct {
	ID      string
	Content []byte
	Ack     func(context.Context) error
}
```

Take a look at the `Ack` field: it's an acknowledge function that should be called after the message has been published successfully.

If you take a glance at the relay's `Run()` method, you can see that it's pretty simple.

Now let's see how deep the rabbit hole goes in the Postgres implementation of `CDCSource` in `solution_v2/internal/cdc/postgres.go`. It's a lot of stuff, isn't it?

It's not a complete solution: here we only handle **transaction** messages, which is not full CDC. A real CDC consumer has to expect many kinds of messages (a simple INSERT or UPDATE, for example), but this example is enough to understand the main concept. In practice we would usually use an existing CDC solution like Debezium. But since we're all hungry for new knowledge, let's try writing a small one ourselves.

First, let's look at the fields. We have `pending` and `ready` slices of `CDCEvent`. Why? To understand that, we need to go deeper into **how transactions look in the replication stream**. In Postgres every statement runs inside a transaction, even a single `INSERT` without an explicit `BEGIN` (it's an implicit transaction). In the logical replication stream, each transaction arrives as a BEGIN message, then its changes and messages, then a COMMIT message. So for our transaction we will receive:

- BEGIN
- MESSAGE
- COMMIT

But what if there are many actions in one transaction? We want to be prepared for that. And don't forget that we receive messages in a loop, so our implementation must be able to parse a whole transaction over several iterations.

One important note before reading the code: to work with replication we use the `pglogrepl` library.

Let's first jump to the `processWALData` function to understand what I'm talking about.
Here we have a big switch statement where we parse the raw WAL bytes and expect a Begin message, a Commit message, or a `LogicalDecodingMessageV2`, which is the message we need to forward to Kafka.

Every message has its own prefix, and we check that the prefix of our message is the one we expect. Then we check that the `inTransaction` field of `PostgresCDC` is true, which means we received a BEGIN message earlier and set it to `true` (it's a state marker meaning we are currently parsing a transaction). Then we parse the content of the message and expect an `outbox.Message`. If that's what we get, we create a `CDCEvent` and put it into the `PostgresCDC.pending` slice. Why not return it to the relay right away? I did it this way to keep the order of messages and because only at commit time do we know the LSN of the end of the transaction, which is where we want to move the replication slot. That's exactly what happens when we receive a `CommitMessage`: for the last event of the transaction, I set an `Ack` function that sends the current replication status to Postgres (take a look at the `sendStandByStatus` function). Postgres expects us to report the WAL position we have processed up to, so it can delete everything before it.

After setting the `Ack` function on the commit, we move everything from `pending` to `ready`, because this transaction is finished.

Next, let's look at `handleMessage`, especially at the other switch statement.
Here we parse the raw message from Postgres and check its type. If it's a `KeepAlive` message with the "reply requested" flag, we send our current status via the already familiar `sendStandByStatus` function, which reports the WAL position we have processed up to. Postgres asks for a reply when it hasn't heard from us for a while, and if no status arrives within `wal_sender_timeout` (60 seconds by default), it closes the connection.

Otherwise, we parse the message and pass it to `processWALData`.

Now let's go up to the `Next` method, which is the one we expose. Here we run an infinite loop. First, we return ready events:

```go
		if len(pcdc.ready) > 0 {
			event := pcdc.ready[0]
			pcdc.ready = pcdc.ready[1:]

			return event, nil
		}
```

Then we check whether the context is still alive. This way, the caller of `Next` first receives all events that are already ready. It's something like a graceful shutdown for `PostgresCDC`: before finishing, it hands over everything it has already processed.

Then we receive a message from the slot and parse it.

All the logic for working with `pglogrepl` is in `internal/replication/replication.go`.

`main.go` for this approach is almost the same, but here we create two Postgres connections: a pool, and a `*pgconn.PgConn` with the `?replication=database` parameter in the connection URI. Then we run two goroutines in an `errgroup`, as in the first approach.

## Summary

We have examined two approaches to implementing the outbox pattern. I would like to reiterate that these are illustrative examples; they certainly need further refinement, for instance, to handle the case where the relay crashes after a message has been sent to Kafka but before it was marked as sent (or acknowledged). Both approaches give **at-least-once** delivery, so consumers should be idempotent (for example, deduplicate by event ID).

For the CDC approach, it is obviously best to use an off-the-shelf solution like Debezium.

If you find something wrong with the solution, I would be happy if you proposed a better option via a pull request.

## Further reading

- https://microservices.io/patterns/data/transactional-outbox.html
- https://www.postgresql.org/docs/current/logical-replication.html
- https://www.postgresql.org/docs/current/runtime-config-replication.html
- https://neon.com/docs/guides/logical-replication-concepts