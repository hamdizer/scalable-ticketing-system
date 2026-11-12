package com.example.ticketing.seat;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Releases seats whose temporary hold expired without payment.
 * Safe to run on every instance: both statements are conditional (status = 'HELD'),
 * so a seat that was BOOKED in the meantime is never touched.
 */
@Component
public class HoldExpiryWorker {

    private final JdbcTemplate jdbc;

    public HoldExpiryWorker(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void releaseExpired() {
        jdbc.update("""
            UPDATE bookings b SET status = 'EXPIRED'
             WHERE b.status = 'PENDING_PAYMENT'
               AND EXISTS (SELECT 1 FROM seats s
                            WHERE s.booking_id = b.id AND s.status = 'HELD' AND s.held_until < now())
            """);
        jdbc.update("""
            UPDATE seats SET status = 'AVAILABLE', booking_id = NULL, held_until = NULL
             WHERE status = 'HELD' AND held_until < now()
            """);
    }
}
