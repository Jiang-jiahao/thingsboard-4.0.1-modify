package com.jnks.iot.server.transport.lwm2m.server.downlink.composite;

import lombok.Getter;
import com.jnks.iot.server.transport.lwm2m.server.downlink.HasVersionedIds;
import com.jnks.iot.server.transport.lwm2m.server.downlink.TbLwM2MDownlinkRequest;

public abstract class AbstractTbLwM2MTargetedDownlinkCompositeRequest<T> implements TbLwM2MDownlinkRequest<T>, HasVersionedIds {

    @Getter
    private final String [] versionedIds;
    @Getter
    private final long timeout;

    public AbstractTbLwM2MTargetedDownlinkCompositeRequest(String [] versionedIds, long timeout) {
        this.versionedIds = versionedIds;
        this.timeout = timeout;
    }

}
