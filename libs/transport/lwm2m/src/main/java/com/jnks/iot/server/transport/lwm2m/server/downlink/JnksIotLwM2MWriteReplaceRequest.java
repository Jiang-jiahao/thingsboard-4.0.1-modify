package com.jnks.iot.server.transport.lwm2m.server.downlink;

import lombok.Builder;
import lombok.Getter;
import org.eclipse.leshan.core.request.ContentFormat;
import org.eclipse.leshan.core.response.WriteResponse;
import com.jnks.iot.server.transport.lwm2m.server.LwM2MOperationType;

public class JnksIotLwM2MWriteReplaceRequest extends AbstractJnksIotLwM2MTargetedDownlinkRequest<WriteResponse> {

    @Getter
    private final ContentFormat contentFormat;
    @Getter
    private final Object value;

    @Builder
    private JnksIotLwM2MWriteReplaceRequest(String versionedId, long timeout, ContentFormat contentFormat, Object value) {
        super(versionedId, timeout);
        this.contentFormat = contentFormat;
        this.value = value;
    }

    @Override
    public LwM2MOperationType getType() {
        return LwM2MOperationType.WRITE_REPLACE;
    }



}
