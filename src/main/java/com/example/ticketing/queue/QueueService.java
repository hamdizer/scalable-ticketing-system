package com.example.ticketing.queue;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

/**
 * Virtual waiting room backed by Redis.
 *
 *  ticket-queue   (ZSET) member=userId, score=arrival sequence  -> FIFO order
 *  ticket-active  (ZSET) member=userId, score=expiry epoch ms   -> users currently allowed to book
 *  admitted:{id}  (STRING) the access token, with TTL
 *
 * Admission runs as ONE Lua script, so it is atomic even if every app instance runs the scheduler.
 */
@Service
public class QueueService {

    static final String QUEUE = "ticket-queue";
    static final String ACTIVE = "ticket-active";
    static final String SEQ = "ticket-queue-seq";
    static final String ADMITTED = "admitted:";

    private static final String ADMIT_LUA = """
        local now  = tonumber(ARGV[1])
        local max  = tonumber(ARGV[2])
        local ttl  = tonumber(ARGV[3])
        local seed = ARGV[4]
        redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
        local slots = max - redis.call('ZCARD', KEYS[2])
        if slots <= 0 then return {} end
        local users = redis.call('ZRANGE', KEYS[1], 0, slots - 1)
        for _, u in ipairs(users) do
          redis.call('ZREM', KEYS[1], u)
          redis.call('ZADD', KEYS[2], now + ttl * 1000, u)
          redis.call('SET', 'admitted:' .. u, seed .. '-' .. u, 'EX', ttl)
        end
        return users
        """;

    private final StringRedisTemplate redis;
    private final DefaultRedisScript<List> admitScript = new DefaultRedisScript<>(ADMIT_LUA, List.class);
    private final int maxActive;
    private final long tokenTtl;

    public QueueService(StringRedisTemplate redis,
                        @Value("${app.queue.max-active}") int maxActive,
                        @Value("${app.queue.token-ttl-seconds}") long tokenTtl) {
        this.redis = redis;
        this.maxActive = maxActive;
        this.tokenTtl = tokenTtl;
    }

    public QueueStatus join(String userId) {
        if (!Boolean.TRUE.equals(redis.hasKey(ADMITTED + userId))
                && redis.opsForZSet().rank(QUEUE, userId) == null) {
            Long seq = redis.opsForValue().increment(SEQ);
            redis.opsForZSet().addIfAbsent(QUEUE, userId, seq == null ? 0 : seq);
        }
        return status(userId);
    }

    public QueueStatus status(String userId) {
        String token = redis.opsForValue().get(ADMITTED + userId);
        if (token != null) return new QueueStatus("ADMITTED", 0L, token);
        Long rank = redis.opsForZSet().rank(QUEUE, userId);
        if (rank == null) return new QueueStatus("NOT_IN_QUEUE", null, null);
        return new QueueStatus("WAITING", rank + 1, null);
    }

    public boolean isAdmitted(String userId, String token) {
        return token != null && token.equals(redis.opsForValue().get(ADMITTED + userId));
    }

    /** Every second, let the next batch of waiting users in (up to max-active concurrent bookers). */
    @Scheduled(fixedDelayString = "${app.queue.admit-interval-ms}")
    public void admitNextBatch() {
        try {
            redis.execute(admitScript, List.of(QUEUE, ACTIVE),
                    String.valueOf(System.currentTimeMillis()),
                    String.valueOf(maxActive),
                    String.valueOf(tokenTtl),
                    UUID.randomUUID().toString());
        } catch (Exception e) {
            // transient Redis problem, try again next tick
        }
    }
}
