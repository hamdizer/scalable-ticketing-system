package com.example.ticketing.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Duration;

/**
 * Distributed fixed-window rate limiter. The counter lives in Redis, so the limit
 * is shared by ALL application instances behind the load balancer.
 */
@Component
public class RateLimitFilter extends OncePerRequestFilter {

    private final StringRedisTemplate redis;
    private final int limitPerSecond;

    public RateLimitFilter(StringRedisTemplate redis, @Value("${app.rate-limit.per-second}") int limitPerSecond) {
        this.redis = redis;
        this.limitPerSecond = limitPerSecond;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/api/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        String client = firstNonBlank(req.getHeader("X-User-Id"), req.getHeader("X-Client-Id"),
                forwardedFor(req), req.getRemoteAddr());
        String key = "rl:" + client + ":" + (System.currentTimeMillis() / 1000);
        try {
            Long count = redis.opsForValue().increment(key);
            if (count != null && count == 1) redis.expire(key, Duration.ofSeconds(2));
            if (count != null && count > limitPerSecond) {
                res.setStatus(429);
                res.setHeader("Retry-After", "1");
                res.setContentType("application/json");
                res.getWriter().write("{\"error\":\"Too many requests\"}");
                return;
            }
        } catch (Exception e) {
            // Fail open: if Redis is down we prefer serving users over blocking everyone.
            logger.warn("Rate limiter unavailable: " + e.getMessage());
        }
        chain.doFilter(req, res);
    }

    private static String forwardedFor(HttpServletRequest req) {
        String xff = req.getHeader("X-Forwarded-For");
        return xff == null ? null : xff.split(",")[0].trim();
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) if (v != null && !v.isBlank()) return v;
        return "unknown";
    }
}
