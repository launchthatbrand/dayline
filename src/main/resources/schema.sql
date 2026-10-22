CREATE TABLE IF NOT EXISTS guests (
  id CHAR(36) NOT NULL PRIMARY KEY,
  name VARCHAR(160) NOT NULL,
  UNIQUE KEY guests_name (name)
);

CREATE TABLE IF NOT EXISTS windows (
  id CHAR(36) NOT NULL PRIMARY KEY,
  attraction VARCHAR(160) NOT NULL,
  starts_at VARCHAR(40) NOT NULL,
  capacity INT NOT NULL,
  remaining INT NOT NULL,
  UNIQUE KEY window_slot (attraction, starts_at)
);

CREATE TABLE IF NOT EXISTS reservations (
  id CHAR(36) NOT NULL PRIMARY KEY,
  guest_id CHAR(36) NOT NULL,
  window_id CHAR(36) NOT NULL,
  guest_name VARCHAR(160) NOT NULL,
  attraction VARCHAR(160) NOT NULL,
  starts_at VARCHAR(40) NOT NULL,
  status VARCHAR(20) NOT NULL,
  source VARCHAR(20) NOT NULL,
  created_at VARCHAR(40) NOT NULL,
  UNIQUE KEY reservation_guest_window (guest_id, window_id)
);

CREATE TABLE IF NOT EXISTS taps (
  idempotency_key VARCHAR(220) NOT NULL PRIMARY KEY,
  guest_id CHAR(36) NOT NULL,
  window_id CHAR(36) NOT NULL,
  tap_id VARCHAR(80) NOT NULL,
  result VARCHAR(20) NOT NULL,
  reservation_id CHAR(36) NULL,
  created_at VARCHAR(40) NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
  id CHAR(36) NOT NULL PRIMARY KEY,
  event_id CHAR(36) NOT NULL,
  guest_id CHAR(36) NOT NULL,
  event_type VARCHAR(40) NOT NULL,
  payload TEXT NOT NULL,
  published TINYINT NOT NULL DEFAULT 0,
  created_at VARCHAR(40) NOT NULL,
  KEY outbox_unpublished (published, created_at)
);

CREATE TABLE IF NOT EXISTS dead_letters (
  event_id CHAR(36) NOT NULL PRIMARY KEY,
  guest_id CHAR(36) NOT NULL,
  event_type VARCHAR(40) NOT NULL,
  payload TEXT NOT NULL,
  error_message VARCHAR(500) NOT NULL,
  created_at VARCHAR(40) NOT NULL
);

CREATE TABLE IF NOT EXISTS consumer_control (
  id INT NOT NULL PRIMARY KEY,
  fail_next TINYINT NOT NULL
);

INSERT IGNORE INTO consumer_control (id, fail_next) VALUES (1, 0);
