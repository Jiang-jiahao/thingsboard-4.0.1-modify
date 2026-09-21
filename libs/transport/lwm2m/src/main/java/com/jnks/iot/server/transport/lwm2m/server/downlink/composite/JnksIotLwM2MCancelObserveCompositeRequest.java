package com.jnks.iot.server.transport.lwm2m.server.downlink.composite;

import lombok.Builder;
import com.jnks.iot.server.transport.lwm2m.server.LwM2MOperationType;

public class JnksIotLwM2MCancelObserveCompositeRequest extends AbstractJnksIotLwM2MTargetedDownlinkCompositeRequest {

    @Builder
    private JnksIotLwM2MCancelObserveCompositeRequest(String [] versionedIds, long timeout) {
        super(versionedIds, timeout);
    }

    @Override
    public LwM2MOperationType getType() {
        return LwM2MOperationType.OBSERVE_COMPOSITE_CANCEL;
    }
}
