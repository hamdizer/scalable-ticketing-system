package com.example.ticketing.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Writes an event row. Call it INSIDE the business transaction so both commit (or roll back) together. */
@Service
public class OutboxService {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public OutboxService(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void add(String topic, String key, Object payload) {
        try {
            jdbc.update("INSERT INTO outbox(topic, msg_key, payload) VALUES (?, ?, ?)",
                    topic, key, mapper.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
