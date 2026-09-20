package com.jnks.iot.server.common.data.exception;

import com.jnks.iot.server.common.data.limit.LimitedApi;

public class RateLimitExceededException extends AbstractRateLimitException {

    public RateLimitExceededException(String message) {
        super(message);
    }

    public RateLimitExceededException(LimitedApi api) {
        super("Rate limit for " + api.getLabel() + " is exceeded");
    }

}
