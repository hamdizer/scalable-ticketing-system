package com.example.ticketing.seat;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/events")
public class EventController {

    private final SeatService seats;
    private final StringRedisTemplate redis;

    public EventController(SeatService seats, StringRedisTemplate redis) {
        this.seats = seats;
        this.redis = redis;
    }

    /** Read-heavy endpoint: cached in Redis for 1 second so 100k refreshes do not become 100k SQL queries. */
    @GetMapping("/{eventId}/stock")
    public Map<String, Object> stock(@PathVariable long eventId) {
        String key = "stock:" + eventId;
        String cached = redis.opsForValue().get(key);
        int available;
        if (cached != null) {
            available = Integer.parseInt(cached);
        } else {
            available = seats.countAvailable(eventId);
            redis.opsForValue().set(key, String.valueOf(available), Duration.ofSeconds(1));
        }
        return Map.of("eventId", eventId, "available", available, "cachedFor", "1s");
    }

    @GetMapping("/{eventId}/seats")
    public List<String> availableSeats(@PathVariable long eventId, @RequestParam(defaultValue = "20") int limit) {
        return seats.listAvailable(eventId, limit);
    }
}
