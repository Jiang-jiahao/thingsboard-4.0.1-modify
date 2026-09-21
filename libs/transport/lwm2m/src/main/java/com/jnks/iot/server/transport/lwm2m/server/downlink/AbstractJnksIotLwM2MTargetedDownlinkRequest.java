package com.jnks.iot.server.transport.lwm2m.server.downlink;

import lombok.Getter;

public abstract class AbstractJnksIotLwM2MTargetedDownlinkRequest<T> implements JnksIotLwM2MDownlinkRequest<T>, HasVersionedId {

    @Getter
    private final String versionedId;
    @Getter
    private final long timeout;

    public AbstractJnksIotLwM2MTargetedDownlinkRequest(String versionedId, long timeout) {
        this.versionedId = versionedId;
        this.timeout = timeout;
    }

}
