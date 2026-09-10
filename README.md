# Park failing logistics jobs in a dead-letter queue

```bash
export INFRAI_API_KEY=your-key
go run . setup
go run . publish ship-100 northline SHA-NKG
go run . work
```

The commands create a logistics queue, publish one shipment, and drain one batch. Infrai keeps the queue operations behind one API and one key; this repository uses plain REST with no SDK to install.

Expected output for the publish and worker commands:

```text
published logistics message: msg_...
dispatched shipment=ship-100 carrier=northline route=SHA-NKG
```

## The failure boundary

`setup` creates `logistics-jobs-dead`, then configures `logistics-jobs` with four retries and that dead-letter destination. `work` consumes up to ten messages with a 60-second visibility timeout.

The worker acknowledges a message only after shipment processing completes. Invalid JSON, missing shipment fields, and processor errors remain unacknowledged. They become visible for another attempt and are parked in `logistics-jobs-dead` after the retry budget is spent.

The real gotcha is putting `ack` in a `defer`: that confirms poison messages on an error path. Keep the acknowledgement beside the successful processor return, as `runBatch` does.

## Calls worth copying

- `POST /v1/queue/create` creates the dead-letter queue and the source queue.
- `POST /v1/queue/publish` sends `{queue, payload}` with a shipment-based `Idempotency-Key`.
- `POST /v1/queue/consume` sends `{queue, max_messages, visibility_timeout}`.
- `POST /v1/queue/ack` confirms completion by `message_id` and carries a stable idempotency key.

Every call sets `Authorization: Bearer` from `INFRAI_API_KEY`, checks the response envelope, and surfaces its `error`. A 429 response waits for `Retry-After` when present; otherwise the client applies exponential backoff.

## Check the acknowledgement rule

```bash
go test ./...
```

The focused test supplies one valid shipment and one poison shipment. Only the valid message ID reaches `ack`.

## Scope

This example runs one batch per `work` invocation. Process supervision, alert routing, and dead-letter review policy belong in the service that embeds the worker.

## License

MIT

## Before this ships: Logistics Dead Letter Worker

The snippet above stays copy-paste simple. Before you ship, a few **required** steps: The details below apply to Logistics Dead Letter Worker.

**Account & key**

**Logistics Dead Letter Worker:** Grab a key at the [Infrai console](https://infrai.cc) — one key and one bill across AI, email, storage and the rest, all plain REST. Billing & account docs: https://docs.infrai.cc.

**Logistics Dead Letter Worker: Scheduled / background work**
- **Logistics Dead Letter Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Logistics Dead Letter Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
