package com.jnks.iot.server.transport.lwm2m.server.downlink.composite;

import lombok.Builder;
import lombok.Getter;
import org.eclipse.leshan.core.request.ContentFormat;
import org.eclipse.leshan.core.response.ObserveCompositeResponse;
import com.jnks.iot.server.transport.lwm2m.server.LwM2MOperationType;
import com.jnks.iot.server.transport.lwm2m.server.downlink.HasContentFormat;

import java.util.Optional;

public class JnksIotLwM2MObserveCompositeRequest extends AbstractJnksIotLwM2MTargetedDownlinkCompositeRequest<ObserveCompositeResponse> implements HasContentFormat {


    private final Optional<ContentFormat> requestContentFormatOpt;

    @Getter
    private final ContentFormat responseContentFormat;

    @Builder
    private JnksIotLwM2MObserveCompositeRequest(String [] versionedIds, long timeout, ContentFormat requestContentFormat, ContentFormat responseContentFormat) {
        super(versionedIds, timeout);
        this.requestContentFormatOpt = Optional.ofNullable(requestContentFormat);
        this.responseContentFormat = responseContentFormat;
    }

    @Override
    public LwM2MOperationType getType() {
        return LwM2MOperationType.OBSERVE_COMPOSITE;
    }

    @Override
    public Optional<ContentFormat> getRequestContentFormat() {
        return this.requestContentFormatOpt;
    }
}
