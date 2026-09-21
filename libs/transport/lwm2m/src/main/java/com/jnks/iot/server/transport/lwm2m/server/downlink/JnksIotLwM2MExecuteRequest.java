package com.jnks.iot.server.transport.lwm2m.server.downlink;

import lombok.Builder;
import lombok.Getter;
import org.eclipse.leshan.core.response.ReadResponse;
import com.jnks.iot.server.transport.lwm2m.server.LwM2MOperationType;

public class JnksIotLwM2MExecuteRequest extends AbstractJnksIotLwM2MTargetedDownlinkRequest<ReadResponse> {

    @Getter
    private final Object params;

    @Builder
    private JnksIotLwM2MExecuteRequest(String versionedId, long timeout, Object params) {
        super(versionedId, timeout);
        this.params = params;
    }

    @Override
    public LwM2MOperationType getType() {
        return LwM2MOperationType.EXECUTE;
    }



}
