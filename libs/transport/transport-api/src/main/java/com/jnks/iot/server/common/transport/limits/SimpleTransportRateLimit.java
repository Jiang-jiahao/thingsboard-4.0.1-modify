package com.jnks.iot.server.common.transport.limits;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.msg.tools.JnksIotRateLimits;

@RequiredArgsConstructor
public class SimpleTransportRateLimit implements TransportRateLimit {

    private final JnksIotRateLimits rateLimit;
    @Getter
    private final String configuration;

    public SimpleTransportRateLimit(String configuration) {
        this.configuration = configuration;
        this.rateLimit = new JnksIotRateLimits(configuration);
    }

    @Override
    public boolean tryConsume() {
        return rateLimit.tryConsume();
    }

    @Override
    public boolean tryConsume(long number) {
        return number <= 0 || rateLimit.tryConsume(number);
    }
}
