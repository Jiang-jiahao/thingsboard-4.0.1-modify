package com.jnks.iot.server.common.data.exception;

public class ApiUsageLimitsExceededException extends AbstractRateLimitException {
    public ApiUsageLimitsExceededException(String message) {
        super(message);
    }

    public ApiUsageLimitsExceededException() {
    }
}
