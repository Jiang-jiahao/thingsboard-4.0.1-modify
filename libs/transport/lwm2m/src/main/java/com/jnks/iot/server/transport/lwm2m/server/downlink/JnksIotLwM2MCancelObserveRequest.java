package com.jnks.iot.server.transport.lwm2m.server.downlink;

import lombok.Builder;
import com.jnks.iot.server.transport.lwm2m.server.LwM2MOperationType;

public class JnksIotLwM2MCancelObserveRequest extends AbstractJnksIotLwM2MTargetedDownlinkRequest<Integer> {

    @Builder
    private JnksIotLwM2MCancelObserveRequest(String versionedId, long timeout) {
        super(versionedId, timeout);
    }

    @Override
    public LwM2MOperationType getType() {
        return LwM2MOperationType.OBSERVE_CANCEL;
    }



}
