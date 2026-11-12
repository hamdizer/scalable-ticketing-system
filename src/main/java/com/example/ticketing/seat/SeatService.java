package com.example.ticketing.seat;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SeatService {

    private final JdbcTemplate jdbc;
    private final double holdSeconds;

    public SeatService(JdbcTemplate jdbc, @Value("${app.seat.hold-seconds}") int holdSeconds) {
        this.jdbc = jdbc;
        this.holdSeconds = holdSeconds;
    }

    /**
     * THE anti-double-booking statement.
     * Condition check + update happen atomically inside PostgreSQL (row lock).
     * If two requests race for the same seat, exactly one gets 1 updated row; the other gets 0.
     */
    public boolean tryHold(long eventId, String seatNumber, String bookingId) {
        int rows = jdbc.update("""
            UPDATE seats
               SET status = 'HELD', booking_id = ?, held_until = now() + make_interval(secs => ?)
             WHERE event_id = ? AND seat_number = ? AND status = 'AVAILABLE'
            """, bookingId, holdSeconds, eventId, seatNumber);
        return rows == 1;
    }

    public int countAvailable(long eventId) {
        Integer n = jdbc.queryForObject(
                "SELECT count(*) FROM seats WHERE event_id = ? AND status = 'AVAILABLE'", Integer.class, eventId);
        return n == null ? 0 : n;
    }

    public List<String> listAvailable(long eventId, int limit) {
        return jdbc.queryForList(
                "SELECT seat_number FROM seats WHERE event_id = ? AND status = 'AVAILABLE' ORDER BY id LIMIT ?",
                String.class, eventId, Math.min(limit, 200));
    }
}
