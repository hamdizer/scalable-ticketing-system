package com.example.ticketing.outbox;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Relays outbox rows to Kafka (at-least-once).
 * FOR UPDATE SKIP LOCKED lets several app instances poll the same table without
 * publishing the same rows twice at the same moment.
 * If Kafka is down the transaction rolls back and the rows are retried on the next tick.
 */
@Component
public class OutboxPublisher {

    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;

    public OutboxPublisher(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka) {
        this.jdbc = jdbc;
        this.kafka = kafka;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-ms}")
    @Transactional
    public void publishPending() throws Exception {
        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT id, topic, msg_key, payload FROM outbox
             WHERE published = FALSE ORDER BY id LIMIT 200 FOR UPDATE SKIP LOCKED
            """);
        if (rows.isEmpty()) return;

        List<CompletableFuture<?>> sends = new ArrayList<>();
        List<Object[]> ids = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            sends.add(kafka.send((String) r.get("topic"), (String) r.get("msg_key"), (String) r.get("payload")));
            ids.add(new Object[]{r.get("id")});
        }
        CompletableFuture.allOf(sends.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
        jdbc.batchUpdate("UPDATE outbox SET published = TRUE WHERE id = ?", ids);
    }
}
