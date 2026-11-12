package com.example.ticketing.payment;

import com.example.ticketing.outbox.OutboxService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Simulated payment service (stands in for Stripe etc.).
 * Idempotency: the payments table has a primary key on the idempotency key, so a redelivered
 * Kafka message can never cause a second charge.
 */
@Component
public class PaymentConsumer {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final OutboxService outbox;
    private final TransactionTemplate tx;
    private final int successPercent;

    public PaymentConsumer(JdbcTemplate jdbc, ObjectMapper mapper, OutboxService outbox, TransactionTemplate tx,
                           @Value("${app.payment.success-percent}") int successPercent) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.outbox = outbox;
        this.tx = tx;
        this.successPercent = successPercent;
    }

    @KafkaListener(topics = "ticket-reservations", groupId = "payment-service")
    public void onReservation(String payload) throws Exception {
        JsonNode n = mapper.readTree(payload);
        String bookingId = n.get("bookingId").asText();
        String idempotencyKey = "pay-" + bookingId;

        // Claim the key. 0 rows => this message was already handled => skip (idempotent consumer).
        int claimed = jdbc.update("""
            INSERT INTO payments(idempotency_key, booking_id, status) VALUES (?, ?, 'PROCESSING')
            ON CONFLICT DO NOTHING
            """, idempotencyKey, bookingId);
        if (claimed == 0) return;

        Thread.sleep(ThreadLocalRandom.current().nextInt(100, 400));   // pretend to call the gateway
        boolean ok = ThreadLocalRandom.current().nextInt(100) < successPercent;

        // Update payment + write result event atomically (outbox again).
        tx.executeWithoutResult(status -> {
            jdbc.update("UPDATE payments SET status = ? WHERE idempotency_key = ?",
                    ok ? "SUCCEEDED" : "FAILED", idempotencyKey);
            outbox.add("payment-events", bookingId, Map.of(
                    "eventType", ok ? "PAYMENT_SUCCEEDED" : "PAYMENT_FAILED",
                    "bookingId", bookingId));
        });
    }
}
