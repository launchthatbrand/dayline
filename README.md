# Dayline

A guest books a return window, a hotel system can create that booking in XML, and a turnstile tap redeems it once. MySQL is the ledger. A reactive Kafka consumer projects the guest's day into DynamoDB, which is what the phone reads.

The command API is Spring MVC. A reservation needs a transaction and a conditional update, so that path stays blocking. The stream process is the reactive part: Reactor Kafka hands the consumer a `Flux`, and the itinerary write uses `DynamoDbAsyncClient` without calling `join()` or `block()`.

## What a tap does

`POST /api/taps` uses the idempotency key `guestId:windowId:tapId`. The first tap for a booked window marks it redeemed and writes an outbox row in the same transaction. The same tap again returns the original result and does not publish. A tap with no booked window is rejected and does not change inventory.

Booking decrements `remaining` with `UPDATE ... WHERE remaining > 0`. Cancelling a booked window puts the seat back. Replay writes a new outbox row for the same event id and does not touch inventory.

The publisher sends `guest.events`, keyed by guest id, so one guest stays in order. A projection that keeps failing is written to MySQL and to `guest.events.dlq`. The operator can arm the fail switch, watch that happen, and replay.

## Run the tests

The tests start MySQL, Kafka, and DynamoDB Local with Testcontainers.

```bash
mvn test
```

Java 21 is required.

## Run it locally

```bash
docker compose up -d
mvn spring-boot:run
```

In another shell, from `web/`:

```bash
pnpm install
API_ORIGIN=http://localhost:8080 pnpm dev
```

The console is http://localhost:3000.

`load/park-open.js` is a k6 script for a park-open burst. It is not part of CI.

```bash
k6 run load/park-open.js
```

## On the cluster

The running console is https://dayline.apps.onpoint.host. One image serves both processes: `DAYLINE_MODE=api` publishes the outbox, and `DAYLINE_MODE=stream` runs the consumer. Images are `ghcr.io/launchthatbrand/dayline-api` and `ghcr.io/launchthatbrand/dayline-web`.

MySQL, Kafka in KRaft mode, and DynamoDB Local run in the `launchthat` namespace. `dayline-env` holds `MYSQL_PASSWORD` and is created on the node. It is not in this repo.

The GitHub Action runs the Testcontainers suite, builds the images, and rolls the deployments. That is the same build, verify, and deploy shape Harness would run. The production equivalents of the local pieces are RDS for MySQL, Amazon MSK, and DynamoDB.
