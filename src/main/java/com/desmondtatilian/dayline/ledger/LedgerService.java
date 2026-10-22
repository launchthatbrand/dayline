package com.desmondtatilian.dayline.ledger;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
public class LedgerService {

    private final JdbcTemplate jdbc;
    private final ObjectMapper json;

    public LedgerService(JdbcTemplate jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    @Transactional
    public Views.WindowView createWindow(String attraction, String startsAt, int capacity) {
        requireText(attraction, "attraction");
        requireText(startsAt, "startsAt");
        if (capacity < 1) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "capacity must be at least 1");
        }
        Views.WindowView existing = findWindow(attraction, startsAt);
        if (existing != null) {
            return existing;
        }
        String id = UUID.randomUUID().toString();
        jdbc.update(
                "INSERT INTO windows (id, attraction, starts_at, capacity, remaining) VALUES (?, ?, ?, ?, ?)",
                id,
                attraction,
                startsAt,
                capacity,
                capacity);
        return new Views.WindowView(id, attraction, startsAt, capacity, capacity);
    }

    @Transactional
    public Views.ReservationView book(String guestName, String windowId, String source) {
        requireText(guestName, "guestName");
        requireText(windowId, "windowId");
        WindowRow window = requireWindow(windowId);
        String guestId = guestId(guestName);
        Views.ReservationView existing = findReservation(guestId, windowId);
        if (existing != null) {
            return existing;
        }
        int updated = jdbc.update(
                "UPDATE windows SET remaining = remaining - 1 WHERE id = ? AND remaining > 0",
                windowId);
        if (updated == 0) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "That return window is full");
        }
        String reservationId = UUID.randomUUID().toString();
        String createdAt = Instant.now().toString();
        jdbc.update(
                """
                INSERT INTO reservations
                  (id, guest_id, window_id, guest_name, attraction, starts_at, status, source, created_at)
                VALUES (?, ?, ?, ?, ?, ?, 'BOOKED', ?, ?)
                """,
                reservationId,
                guestId,
                windowId,
                guestName,
                window.attraction(),
                window.startsAt(),
                source,
                createdAt);
        enqueue(
                guestId,
                "reservation.booked",
                event(guestId, guestName, window, reservationId, "BOOKED", "reservation.booked"));
        return new Views.ReservationView(
                reservationId,
                guestId,
                guestName,
                windowId,
                window.attraction(),
                window.startsAt(),
                "BOOKED",
                source);
    }

    @Transactional
    public Views.ReservationView cancel(String reservationId) {
        Views.ReservationView reservation = findReservationById(reservationId);
        if (reservation == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Reservation not found");
        }
        int updated = jdbc.update(
                "UPDATE reservations SET status = 'CANCELLED' WHERE id = ? AND status = 'BOOKED'",
                reservationId);
        if (updated == 0) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Only a booked return window can be cancelled");
        }
        jdbc.update("UPDATE windows SET remaining = remaining + 1 WHERE id = ?", reservation.windowId());
        WindowRow window = requireWindow(reservation.windowId());
        enqueue(
                reservation.guestId(),
                "reservation.cancelled",
                event(reservation.guestId(), reservation.guestName(), window, reservationId, "CANCELLED", "reservation.cancelled"));
        return new Views.ReservationView(
                reservation.id(),
                reservation.guestId(),
                reservation.guestName(),
                reservation.windowId(),
                reservation.attraction(),
                reservation.startsAt(),
                "CANCELLED",
                reservation.source());
    }

    @Transactional
    public Views.TapView tap(String guestId, String windowId, String tapId) {
        requireText(guestId, "guestId");
        requireText(windowId, "windowId");
        requireText(tapId, "tapId");
        String key = guestId + ":" + windowId + ":" + tapId;
        Views.TapView existing = findTap(key);
        if (existing != null) {
            return existing;
        }
        Views.ReservationView reservation = findReservation(guestId, windowId);
        String reservationId = reservation == null ? null : reservation.id();
        String result = "REJECTED";
        if (reservation != null && "BOOKED".equals(reservation.status())) {
            int updated = jdbc.update(
                    "UPDATE reservations SET status = 'REDEEMED' WHERE id = ? AND status = 'BOOKED'",
                    reservation.id());
            if (updated == 1) {
                result = "REDEEMED";
            }
        }
        String createdAt = Instant.now().toString();
        jdbc.update(
                """
                INSERT INTO taps (idempotency_key, guest_id, window_id, tap_id, result, reservation_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                key,
                guestId,
                windowId,
                tapId,
                result,
                reservationId,
                createdAt);
        String guestName = reservation == null ? guestName(guestId) : reservation.guestName();
        WindowRow window = findWindowById(windowId);
        String type = "REDEEMED".equals(result) ? "tap.redeemed" : "tap.rejected";
        if (window != null) {
            enqueue(guestId, type, event(guestId, guestName, window, reservationId, result, type));
        }
        return new Views.TapView(key, guestId, windowId, tapId, result, reservationId);
    }

    @Transactional
    public void replay(String eventId) {
        List<OutboxRow> rows = jdbc.query(
                """
                SELECT id, event_id, guest_id, event_type, payload
                FROM outbox
                WHERE event_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (rs, row) -> new OutboxRow(
                        rs.getString("id"),
                        rs.getString("event_id"),
                        rs.getString("guest_id"),
                        rs.getString("event_type"),
                        rs.getString("payload")),
                eventId);
        if (rows.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Event not found");
        }
        OutboxRow source = rows.get(0);
        jdbc.update(
                """
                INSERT INTO outbox (id, event_id, guest_id, event_type, payload, published, created_at)
                VALUES (?, ?, ?, ?, ?, 0, ?)
                """,
                UUID.randomUUID().toString(),
                source.eventId(),
                source.guestId(),
                source.eventType(),
                source.payload(),
                Instant.now().toString());
    }

    public void setFailNext(boolean failNext) {
        jdbc.update("UPDATE consumer_control SET fail_next = ? WHERE id = 1", failNext ? 1 : 0);
    }

    public Views.StatsView stats() {
        List<Views.WindowView> windows = jdbc.query(
                "SELECT id, attraction, starts_at, capacity, remaining FROM windows ORDER BY starts_at, attraction",
                (rs, row) -> new Views.WindowView(
                        rs.getString("id"),
                        rs.getString("attraction"),
                        rs.getString("starts_at"),
                        rs.getInt("capacity"),
                        rs.getInt("remaining")));
        List<Views.ReservationView> reservations = jdbc.query(
                """
                SELECT id, guest_id, guest_name, window_id, attraction, starts_at, status, source
                FROM reservations
                ORDER BY created_at DESC
                """,
                (rs, row) -> new Views.ReservationView(
                        rs.getString("id"),
                        rs.getString("guest_id"),
                        rs.getString("guest_name"),
                        rs.getString("window_id"),
                        rs.getString("attraction"),
                        rs.getString("starts_at"),
                        rs.getString("status"),
                        rs.getString("source")));
        List<Views.TapView> taps = jdbc.query(
                """
                SELECT idempotency_key, guest_id, window_id, tap_id, result, reservation_id
                FROM taps
                ORDER BY created_at DESC
                """,
                (rs, row) -> new Views.TapView(
                        rs.getString("idempotency_key"),
                        rs.getString("guest_id"),
                        rs.getString("window_id"),
                        rs.getString("tap_id"),
                        rs.getString("result"),
                        rs.getString("reservation_id")));
        List<Views.DeadView> dead = jdbc.query(
                """
                SELECT event_id, guest_id, event_type, error_message, created_at
                FROM dead_letters
                ORDER BY created_at DESC
                """,
                (rs, row) -> new Views.DeadView(
                        rs.getString("event_id"),
                        rs.getString("guest_id"),
                        rs.getString("event_type"),
                        rs.getString("error_message"),
                        rs.getString("created_at")));
        Integer unpublished = jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox WHERE published = 0",
                Integer.class);
        Integer failNext = jdbc.queryForObject(
                "SELECT fail_next FROM consumer_control WHERE id = 1",
                Integer.class);
        return new Views.StatsView(
                windows,
                reservations,
                taps,
                dead,
                unpublished == null ? 0 : unpublished,
                failNext != null && failNext == 1);
    }

    private void enqueue(String guestId, String type, Map<String, Object> body) {
        try {
            jdbc.update(
                    """
                    INSERT INTO outbox (id, event_id, guest_id, event_type, payload, published, created_at)
                    VALUES (?, ?, ?, ?, ?, 0, ?)
                    """,
                    UUID.randomUUID().toString(),
                    body.get("eventId"),
                    guestId,
                    type,
                    json.writeValueAsString(body),
                    Instant.now().toString());
        } catch (com.fasterxml.jackson.core.JsonProcessingException exception) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Could not store the event");
        }
    }

    private Map<String, Object> event(
            String guestId,
            String guestName,
            WindowRow window,
            String reservationId,
            String status,
            String type) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("eventId", UUID.randomUUID().toString());
        body.put("guestId", guestId);
        body.put("guestName", guestName == null ? "" : guestName);
        body.put("type", type);
        body.put("status", status);
        body.put("at", Instant.now().toString());
        body.put("attraction", window.attraction());
        body.put("startsAt", window.startsAt());
        body.put("windowId", window.id());
        body.put("reservationId", reservationId == null ? "" : reservationId);
        return body;
    }

    private String guestId(String name) {
        List<String> ids = jdbc.query(
                "SELECT id FROM guests WHERE name = ?",
                (rs, row) -> rs.getString(1),
                name);
        if (!ids.isEmpty()) {
            return ids.get(0);
        }
        String id = UUID.randomUUID().toString();
        try {
            jdbc.update("INSERT INTO guests (id, name) VALUES (?, ?)", id, name);
            return id;
        } catch (DuplicateKeyException exception) {
            return jdbc.queryForObject("SELECT id FROM guests WHERE name = ?", String.class, name);
        }
    }

    private String guestName(String guestId) {
        List<String> names = jdbc.query(
                "SELECT name FROM guests WHERE id = ?",
                (rs, row) -> rs.getString(1),
                guestId);
        return names.isEmpty() ? "" : names.get(0);
    }

    private WindowRow requireWindow(String windowId) {
        WindowRow window = findWindowById(windowId);
        if (window == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Return window not found");
        }
        return window;
    }

    private WindowRow findWindowById(String windowId) {
        List<WindowRow> rows = jdbc.query(
                "SELECT id, attraction, starts_at FROM windows WHERE id = ?",
                (rs, row) -> new WindowRow(rs.getString("id"), rs.getString("attraction"), rs.getString("starts_at")),
                windowId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private Views.WindowView findWindow(String attraction, String startsAt) {
        List<Views.WindowView> rows = jdbc.query(
                "SELECT id, attraction, starts_at, capacity, remaining FROM windows WHERE attraction = ? AND starts_at = ?",
                (rs, row) -> new Views.WindowView(
                        rs.getString("id"),
                        rs.getString("attraction"),
                        rs.getString("starts_at"),
                        rs.getInt("capacity"),
                        rs.getInt("remaining")),
                attraction,
                startsAt);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private Views.ReservationView findReservation(String guestId, String windowId) {
        List<Views.ReservationView> rows = jdbc.query(
                """
                SELECT id, guest_id, guest_name, window_id, attraction, starts_at, status, source
                FROM reservations
                WHERE guest_id = ? AND window_id = ?
                """,
                this::mapReservation,
                guestId,
                windowId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private Views.ReservationView findReservationById(String reservationId) {
        List<Views.ReservationView> rows = jdbc.query(
                """
                SELECT id, guest_id, guest_name, window_id, attraction, starts_at, status, source
                FROM reservations
                WHERE id = ?
                """,
                this::mapReservation,
                reservationId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private Views.ReservationView mapReservation(java.sql.ResultSet rs, int row) throws java.sql.SQLException {
        return new Views.ReservationView(
                rs.getString("id"),
                rs.getString("guest_id"),
                rs.getString("guest_name"),
                rs.getString("window_id"),
                rs.getString("attraction"),
                rs.getString("starts_at"),
                rs.getString("status"),
                rs.getString("source"));
    }

    private Views.TapView findTap(String key) {
        List<Views.TapView> rows = jdbc.query(
                """
                SELECT idempotency_key, guest_id, window_id, tap_id, result, reservation_id
                FROM taps
                WHERE idempotency_key = ?
                """,
                (rs, row) -> new Views.TapView(
                        rs.getString("idempotency_key"),
                        rs.getString("guest_id"),
                        rs.getString("window_id"),
                        rs.getString("tap_id"),
                        rs.getString("result"),
                        rs.getString("reservation_id")),
                key);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, field + " is required");
        }
    }

    private record WindowRow(String id, String attraction, String startsAt) {
    }

    public record OutboxRow(String id, String eventId, String guestId, String eventType, String payload) {
    }
}
