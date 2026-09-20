package com.jnks.iot.server.common.msg.tools;

import lombok.Getter;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.exception.AbstractRateLimitException;

/**
 * Created by ashvayka on 22.10.18.
 */
public class TbRateLimitsException extends AbstractRateLimitException {
    @Getter
    private final EntityType entityType;

    public TbRateLimitsException(EntityType entityType) {
        super(entityType.name() + " rate limits reached!");
        this.entityType = entityType;
    }

    public TbRateLimitsException(String message) {
        super(message);
        this.entityType = null;
    }

}
