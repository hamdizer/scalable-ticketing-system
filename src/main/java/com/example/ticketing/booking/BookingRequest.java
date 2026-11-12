package com.example.ticketing.booking;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

public record BookingRequest(
        @NotNull Long eventId,
        @NotNull @Pattern(regexp = "^[A-Z][0-9]{1,3}$", message = "seatNumber like A10") String seatNumber) {}
