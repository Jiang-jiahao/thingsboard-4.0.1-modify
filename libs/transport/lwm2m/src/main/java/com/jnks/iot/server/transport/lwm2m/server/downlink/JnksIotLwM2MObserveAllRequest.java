package com.jnks.iot.server.transport.lwm2m.server.downlink;

import lombok.Builder;
import lombok.Getter;
import com.jnks.iot.server.transport.lwm2m.server.LwM2MOperationType;

import java.util.Set;

public class JnksIotLwM2MObserveAllRequest implements JnksIotLwM2MDownlinkRequest<Set<String>> {

    @Getter
    private final long timeout;

    @Builder
    private JnksIotLwM2MObserveAllRequest(long timeout) {
        this.timeout = timeout;
    }

    @Override
    public LwM2MOperationType getType() {
        return LwM2MOperationType.OBSERVE_READ_ALL;
    }



}
