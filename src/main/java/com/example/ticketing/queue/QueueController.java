package com.example.ticketing.queue;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/queue")
public class QueueController {

    private final QueueService queue;

    public QueueController(QueueService queue) { this.queue = queue; }

    @PostMapping("/join")
    public QueueStatus join(@RequestHeader("X-User-Id") String userId) {
        return queue.join(userId);
    }

    @GetMapping("/status")
    public QueueStatus status(@RequestHeader("X-User-Id") String userId) {
        return queue.status(userId);
    }
}
