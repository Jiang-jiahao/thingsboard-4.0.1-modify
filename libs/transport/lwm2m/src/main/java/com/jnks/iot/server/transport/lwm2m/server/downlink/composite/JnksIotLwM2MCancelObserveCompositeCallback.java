package com.jnks.iot.server.transport.lwm2m.server.downlink.composite;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;
import com.jnks.iot.server.transport.lwm2m.server.downlink.AbstractJnksIotLwM2MRequestCallback;
import com.jnks.iot.server.transport.lwm2m.server.log.LwM2MTelemetryLogService;

import static com.jnks.iot.server.transport.lwm2m.utils.LwM2MTransportUtil.LOG_LWM2M_INFO;

@Slf4j
public class JnksIotLwM2MCancelObserveCompositeCallback extends AbstractJnksIotLwM2MRequestCallback<JnksIotLwM2MCancelObserveCompositeRequest, Integer> {

    private final String [] versionedIds;

    public JnksIotLwM2MCancelObserveCompositeCallback(LwM2MTelemetryLogService logService, LwM2mClient client, String [] versionedIds) {
        super(logService, client);
        this.versionedIds = versionedIds;
    }

    @Override
    public void onSuccess(JnksIotLwM2MCancelObserveCompositeRequest request, Integer canceledSubscriptionsCount) {
        log.trace("[{}] Cancel composite observation of [{}] successful: {}", client.getEndpoint(),  this.versionedIds, canceledSubscriptionsCount);
        logService.log(client, String.format("[%s]: Cancel Composite Observe for [%s] successful. Result: [%s]", LOG_LWM2M_INFO, this.versionedIds, canceledSubscriptionsCount));
    }
}
