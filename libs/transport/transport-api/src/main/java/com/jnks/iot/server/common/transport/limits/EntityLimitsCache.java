package com.jnks.iot.server.common.transport.limits;

public interface EntityLimitsCache {

    boolean get(EntityLimitKey key);

    void put(EntityLimitKey key, boolean value);

}
