package com.jnks.iot.server.transport.lwm2m.server.downlink;

import lombok.Builder;
import lombok.Getter;
import com.jnks.iot.server.transport.lwm2m.server.LwM2MOperationType;

public class JnksIotLwM2MDiscoverAllRequest implements JnksIotLwM2MDownlinkRequest<String> {

    @Getter
    private final long timeout;

    @Builder
    private JnksIotLwM2MDiscoverAllRequest(long timeout) {
        this.timeout = timeout;
    }

    @Override
    public LwM2MOperationType getType() {
        return LwM2MOperationType.DISCOVER_ALL;
    }



}
