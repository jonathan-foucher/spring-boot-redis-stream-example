package com.jonathanfoucher.redisstreamexample.common.healthcheck;

import com.jonathanfoucher.redisstreamexample.configs.RedisConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class RedisStreamHealthIndicator implements HealthIndicator {
    private final RedisConfig redisConfig;

    @Override
    public Health health() {
        return (redisConfig.isSubscriptionActive() ? Health.up() : Health.down()).build();
    }
}
