package com.jnks.iot.server.transport.lwm2m.server.downlink.composite;

import lombok.Builder;
import lombok.Getter;
import org.eclipse.leshan.core.request.ContentFormat;
import org.eclipse.leshan.core.response.WriteCompositeResponse;
import com.jnks.iot.server.transport.lwm2m.server.LwM2MOperationType;
import com.jnks.iot.server.transport.lwm2m.server.downlink.AbstractJnksIotLwM2MTargetedDownlinkRequest;

public class JnksIotLwM2MWriteCompositeRequest extends AbstractJnksIotLwM2MTargetedDownlinkRequest<WriteCompositeResponse> {

    @Getter
    private final ContentFormat contentFormat;
    @Getter
    private final Object value;

    @Builder
    private JnksIotLwM2MWriteCompositeRequest(String versionedId, long timeout, ContentFormat contentFormat, Object value) {
        super(versionedId, timeout);
        this.contentFormat = contentFormat;
        this.value = value;
    }

    @Override
    public LwM2MOperationType getType() {
        return LwM2MOperationType.WRITE_REPLACE;
    }



}
