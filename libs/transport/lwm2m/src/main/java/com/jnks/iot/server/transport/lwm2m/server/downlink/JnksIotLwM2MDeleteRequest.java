package com.jnks.iot.server.transport.lwm2m.server.downlink;

import lombok.Builder;
import org.eclipse.leshan.core.response.ReadResponse;
import com.jnks.iot.server.transport.lwm2m.server.LwM2MOperationType;

public class JnksIotLwM2MDeleteRequest extends AbstractJnksIotLwM2MTargetedDownlinkRequest<ReadResponse> {

    @Builder
    private JnksIotLwM2MDeleteRequest(String versionedId, long timeout) {
        super(versionedId, timeout);
    }

    @Override
    public LwM2MOperationType getType() {
        return LwM2MOperationType.DELETE;
    }



}
