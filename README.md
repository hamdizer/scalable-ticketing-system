# Scalable Ticketing System (Spring Boot, Redis, Kafka, PostgreSQL, NGINX)

An educational implementation of a "ticket war" architecture: thousands of users hit a 1,000-seat
event at the same moment, and the system never double-books a seat.

```
 users ──► NGINX (least_conn) ──► app1 ┐
                                  app2 ┤──► Redis      waiting room (ZSET + Lua), rate limit, 1s stock cache
                                       ├──► PostgreSQL seats (atomic UPDATE), bookings, payments, outbox
                                       └──► Kafka      ticket-reservations ► payment ► payment-events ► booking
```

## Run

Requirements: Docker + Docker Compose. (k6 only for the load test.)

```bash
docker compose up --build
curl localhost:8080/api/events/1/stock          # {"available":1000,...}
```
Start-up takes ~30–60 s (Maven build, Kafka boot). The app containers restart automatically if Kafka is not ready yet.

## Try the flow by hand

```bash
# 1. join the waiting room
curl -X POST localhost:8080/api/queue/join   -H 'X-User-Id: alice'
# 2. poll until status = ADMITTED, copy accessToken (takes ~1 s when the room is not full)
curl         localhost:8080/api/queue/status -H 'X-User-Id: alice'
# 3. book a seat
curl -X POST localhost:8080/api/bookings -H 'Content-Type: application/json' \
  -H 'X-User-Id: alice' -H 'X-Access-Token: <token>' -d '{"eventId":1,"seatNumber":"A10"}'
# 4. watch it go PENDING_PAYMENT -> CONFIRMED (or PAYMENT_FAILED, seat released)
curl localhost:8080/api/bookings/<bookingId>
# same seat again => 409
```
Other endpoints: `GET /api/events/1/seats?limit=20`, `GET /actuator/health`.

## Load test

```bash
k6 run load-tests/load-test.js                 # 1000 users, random seats
k6 run -e MODE=hot load-tests/load-test.js     # everybody wants seat A1: exactly ONE 201, the rest 409
```
Afterwards check in PostgreSQL that nothing was double sold:
```bash
docker compose exec postgres psql -U ticketing -c \
 "select status, count(*) from seats group by status; select seat_number, count(*) from bookings where status in ('PENDING_PAYMENT','CONFIRMED') group by 1 having count(*) > 1;"
```
The second query must return zero rows.

## What each piece does and where it lives

| Concept | Code | Idea |
|---|---|---|
| Load balancing | `nginx/nginx.conf` | `least_conn` over app1/app2, failed instances skipped. Apps are stateless, so any instance can serve any request. |
| Virtual queue | `queue/QueueService` | Redis ZSET = FIFO line. A Lua script atomically admits the next users while the number of active bookers < `QUEUE_MAX_ACTIVE`. Admitted users get a token with a TTL. |
| Admission control | `booking/BookingController` | `POST /api/bookings` is refused (403) without a valid token, so the DB only sees what it can handle. |
| Rate limiting | `config/RateLimitFilter` | Redis counter per user per second, shared by all instances → 429. |
| Caching | `seat/EventController` | Stock endpoint cached 1 s in Redis. |
| No double booking | `seat/SeatService.tryHold` | `UPDATE ... WHERE status='AVAILABLE'` is atomic; exactly one racer gets 1 row. |
| Temporary hold | `HoldExpiryWorker` | AVAILABLE → HELD (10 min) → BOOKED / back to AVAILABLE. Conditional updates, safe on every instance. |
| Transactional outbox | `outbox/*`, `BookingService` | Seat hold + booking + event row commit in one transaction; `OutboxPublisher` relays to Kafka (`SKIP LOCKED`, at-least-once). |
| Kafka | `payment/PaymentConsumer`, `booking/BookingResultConsumer` | Booking → `ticket-reservations` → payment → `payment-events` → booking confirmed. 6 partitions, keyed by bookingId. |
| Idempotency | `payments` table PK | A redelivered message cannot charge twice. |
| Edge case | `BookingResultConsumer` | Payment succeeded after the hold expired → `FAILED_NEEDS_REFUND`. |
| Virtual threads | `application.yml` | Java 21 virtual threads for cheap blocking I/O. The Hikari pool (20) is the true DB concurrency limit. |

## How this scales further

- More app instances: add `app3` in compose, or move to Kubernetes with an HPA. Nothing else changes.
- Reads: PostgreSQL read replicas for browsing; keep seat holds on the primary.
- Writes: shard seats by `event_id` once a single primary is the bottleneck.
- Kafka: more partitions and consumers (up to the partition count).
- Redis: Sentinel or Cluster. Note the admission Lua script uses fixed key names plus one dynamic key, so under Redis Cluster use hash tags.
- Observability: Prometheus + Grafana on `/actuator/metrics`.

## Known limitations (be honest in an interview)

- No real authentication: the user id comes from the `X-User-Id` header. Add Spring Security + JWT for production, and bind queue tokens to the authenticated user.
- Payment is simulated (random delay, 90% success).
- A crash between claiming the payment key and writing the result leaves a payment stuck in `PROCESSING`; production needs a reconciliation job.
- The Kafka listener uses default retry behaviour and no dead-letter topic.
- Rate limiter is a simple fixed window (allows brief bursts at window edges).
- Single-node Kafka/Redis/Postgres in compose: this demonstrates the design, not high availability.
- Queue positions are not shuffled or bot-protected.
