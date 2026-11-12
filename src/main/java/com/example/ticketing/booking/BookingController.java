package com.example.ticketing.booking;

import com.example.ticketing.common.ApiException;
import com.example.ticketing.queue.QueueService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/bookings")
public class BookingController {

    private final BookingService bookings;
    private final QueueService queue;

    public BookingController(BookingService bookings, QueueService queue) {
        this.bookings = bookings;
        this.queue = queue;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, Object> book(@RequestHeader("X-User-Id") String userId,
                                    @RequestHeader("X-Access-Token") String token,
                                    @Valid @RequestBody BookingRequest request) {
        // Admission control: only users let in by the virtual queue may reach the expensive part.
        if (!queue.isAdmitted(userId, token)) {
            throw new ApiException(HttpStatus.FORBIDDEN, "Join the queue and wait until you are ADMITTED");
        }
        return bookings.create(userId, request);
    }

    @GetMapping("/{id}")
    public Map<String, Object> get(@PathVariable String id) {
        return bookings.get(id);
    }
}
