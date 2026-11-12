package com.example.ticketing.booking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** Consumes payment results and finalises the booking. All updates are conditional => idempotent on redelivery. */
@Component
public class BookingResultConsumer {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public BookingResultConsumer(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "payment-events", groupId = "booking-service")
    @Transactional
    public void onPaymentResult(String payload) throws Exception {
        JsonNode n = mapper.readTree(payload);
        String bookingId = n.get("bookingId").asText();
        boolean success = "PAYMENT_SUCCEEDED".equals(n.get("eventType").asText());

        if (success) {
            int updated = jdbc.update(
                    "UPDATE bookings SET status = 'CONFIRMED' WHERE id = ? AND status = 'PENDING_PAYMENT'", bookingId);
            if (updated == 1) {
                int seat = jdbc.update("""
                    UPDATE seats SET status = 'BOOKED', held_until = NULL
                     WHERE booking_id = ? AND status = 'HELD'
                    """, bookingId);
                if (seat == 0) {
                    // Paid, but the hold had already expired and the seat may belong to someone else.
                    jdbc.update("UPDATE bookings SET status = 'FAILED_NEEDS_REFUND' WHERE id = ?", bookingId);
                }
            }
        } else {
            jdbc.update("UPDATE bookings SET status = 'PAYMENT_FAILED' WHERE id = ? AND status = 'PENDING_PAYMENT'",
                    bookingId);
            jdbc.update("""
                UPDATE seats SET status = 'AVAILABLE', booking_id = NULL, held_until = NULL
                 WHERE booking_id = ? AND status = 'HELD'
                """, bookingId);
        }
    }
}
