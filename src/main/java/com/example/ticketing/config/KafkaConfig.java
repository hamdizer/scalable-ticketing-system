package com.example.ticketing.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {
    // 6 partitions => up to 6 consumers per group work in parallel.
    // Messages are keyed by bookingId => all events of one booking stay ordered.
    @Bean NewTopic ticketReservations() { return TopicBuilder.name("ticket-reservations").partitions(6).replicas(1).build(); }
    @Bean NewTopic paymentEvents()      { return TopicBuilder.name("payment-events").partitions(6).replicas(1).build(); }
}
