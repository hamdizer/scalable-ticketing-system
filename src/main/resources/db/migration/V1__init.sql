CREATE TABLE events (
  id   BIGINT PRIMARY KEY,
  name VARCHAR(200) NOT NULL
);

CREATE TABLE seats (
  id          BIGSERIAL PRIMARY KEY,
  event_id    BIGINT      NOT NULL REFERENCES events(id),
  seat_number VARCHAR(10) NOT NULL,
  status      VARCHAR(20) NOT NULL DEFAULT 'AVAILABLE',   -- AVAILABLE | HELD | BOOKED
  held_until  TIMESTAMPTZ,
  booking_id  VARCHAR(64),
  UNIQUE (event_id, seat_number)
);
CREATE INDEX idx_seats_status ON seats(event_id, status);
CREATE INDEX idx_seats_held   ON seats(held_until) WHERE status = 'HELD';
CREATE INDEX idx_seats_booking ON seats(booking_id);

CREATE TABLE bookings (
  id          VARCHAR(64) PRIMARY KEY,
  user_id     VARCHAR(100) NOT NULL,
  event_id    BIGINT       NOT NULL,
  seat_number VARCHAR(10)  NOT NULL,
  status      VARCHAR(30)  NOT NULL,  -- PENDING_PAYMENT | CONFIRMED | PAYMENT_FAILED | EXPIRED | FAILED_NEEDS_REFUND
  amount      NUMERIC(10,2) NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE payments (
  idempotency_key VARCHAR(100) PRIMARY KEY,   -- same key => never charged twice
  booking_id      VARCHAR(64) NOT NULL,
  status          VARCHAR(20) NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox: events are written in the SAME transaction as business data
CREATE TABLE outbox (
  id         BIGSERIAL PRIMARY KEY,
  topic      VARCHAR(100) NOT NULL,
  msg_key    VARCHAR(100) NOT NULL,
  payload    TEXT NOT NULL,
  published  BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_outbox_pending ON outbox(id) WHERE published = FALSE;

INSERT INTO events(id, name) VALUES (1, 'Big Concert 2026');

-- 1000 seats: A1..A100, B1..B100, ... J1..J100
INSERT INTO seats(event_id, seat_number)
SELECT 1, chr(65 + (n - 1) / 100) || (((n - 1) % 100) + 1)
FROM generate_series(1, 1000) AS n;
