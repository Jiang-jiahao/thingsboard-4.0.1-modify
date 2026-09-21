package com.jnks.iot.server.transport.lwm2m.server.downlink;

import lombok.Builder;
import lombok.Getter;
import com.jnks.iot.server.transport.lwm2m.server.LwM2MOperationType;

public class JnksIotLwM2MCancelAllRequest implements JnksIotLwM2MDownlinkRequest<Integer> {

    @Getter
    private final long timeout;

    @Builder
    private JnksIotLwM2MCancelAllRequest(long timeout) {
        this.timeout = timeout;
    }

    @Override
    public LwM2MOperationType getType() {
        return LwM2MOperationType.OBSERVE_CANCEL_ALL;
    }

}
