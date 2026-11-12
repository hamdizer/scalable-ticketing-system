package com.example.ticketing.booking;

import com.example.ticketing.common.ApiException;
import com.example.ticketing.outbox.OutboxService;
import com.example.ticketing.seat.SeatService;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class BookingService {

    private static final BigDecimal PRICE = new BigDecimal("150.00");

    private final SeatService seats;
    private final OutboxService outbox;
    private final JdbcTemplate jdbc;

    public BookingService(SeatService seats, OutboxService outbox, JdbcTemplate jdbc) {
        this.seats = seats;
        this.outbox = outbox;
        this.jdbc = jdbc;
    }

    /**
     * ONE transaction: hold seat + insert booking + insert outbox event.
     * Either all three happen or none (solves the "saved to DB but event lost" dual-write problem).
     */
    @Transactional
    public Map<String, Object> create(String userId, BookingRequest req) {
        String bookingId = "BOOK-" + UUID.randomUUID();

        if (!seats.tryHold(req.eventId(), req.seatNumber(), bookingId)) {
            throw new ApiException(HttpStatus.CONFLICT, "Seat " + req.seatNumber() + " is not available");
        }
        jdbc.update("""
            INSERT INTO bookings(id, user_id, event_id, seat_number, status, amount)
            VALUES (?, ?, ?, ?, 'PENDING_PAYMENT', ?)
            """, bookingId, userId, req.eventId(), req.seatNumber(), PRICE);

        outbox.add("ticket-reservations", bookingId, Map.of(
                "eventType", "TICKET_RESERVED",
                "bookingId", bookingId,
                "userId", userId,
                "eventId", req.eventId(),
                "seat", req.seatNumber(),
                "amount", PRICE));

        return Map.of("bookingId", bookingId, "status", "PENDING_PAYMENT", "seat", req.seatNumber());
    }

    public Map<String, Object> get(String bookingId) {
        List<Map<String, Object>> rows = jdbc.queryForList("SELECT * FROM bookings WHERE id = ?", bookingId);
        if (rows.isEmpty()) throw new ApiException(HttpStatus.NOT_FOUND, "Booking not found");
        return rows.get(0);
    }
}
