package com.jnks.iot.server.transport.lwm2m.server.downlink;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;
import com.jnks.iot.server.transport.lwm2m.server.log.LwM2MTelemetryLogService;

import static com.jnks.iot.server.transport.lwm2m.utils.LwM2MTransportUtil.LOG_LWM2M_INFO;

@Slf4j
public class JnksIotLwM2MCancelObserveCallback extends AbstractJnksIotLwM2MRequestCallback<JnksIotLwM2MCancelObserveRequest, Integer> {

    private final String versionedId;

    public JnksIotLwM2MCancelObserveCallback(LwM2MTelemetryLogService logService, LwM2mClient client, String versionedId) {
        super(logService, client);
        this.versionedId = versionedId;
    }

    @Override
    public void onSuccess(JnksIotLwM2MCancelObserveRequest request, Integer canceledSubscriptionsCount) {
        log.trace("[{}] Cancel observation of [{}] successful: {}", client.getEndpoint(),  versionedId, canceledSubscriptionsCount);
        logService.log(client, String.format("[%s]: Cancel Observe for [%s] successful. Result: [%s]", LOG_LWM2M_INFO, versionedId, canceledSubscriptionsCount));
    }

}
