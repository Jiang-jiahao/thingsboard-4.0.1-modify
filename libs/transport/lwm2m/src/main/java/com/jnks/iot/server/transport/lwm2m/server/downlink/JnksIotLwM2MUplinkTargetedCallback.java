package com.jnks.iot.server.transport.lwm2m.server.downlink;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;
import com.jnks.iot.server.transport.lwm2m.server.log.LwM2MTelemetryLogService;
import com.jnks.iot.server.transport.lwm2m.server.uplink.LwM2mUplinkMsgHandler;

@Slf4j
public abstract class JnksIotLwM2MUplinkTargetedCallback<R, T> extends JnksIotLwM2MTargetedCallback<R, T> {

    protected LwM2mUplinkMsgHandler handler;

    public JnksIotLwM2MUplinkTargetedCallback(LwM2mUplinkMsgHandler handler, LwM2MTelemetryLogService logService, LwM2mClient client, String versionedId) {
        super(logService, client, versionedId);
        this.handler = handler;
    }

    public JnksIotLwM2MUplinkTargetedCallback(LwM2mUplinkMsgHandler handler, LwM2MTelemetryLogService logService, LwM2mClient client, String[] versionedIds) {
        super(logService, client, versionedIds);
        this.handler = handler;
    }

}
