package com.jnks.iot.server.transport.lwm2m.server.downlink;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;
import com.jnks.iot.server.transport.lwm2m.server.log.LwM2MTelemetryLogService;

import static com.jnks.iot.server.transport.lwm2m.utils.LwM2MTransportUtil.LOG_LWM2M_INFO;

@Slf4j
public class JnksIotLwM2MCancelAllObserveCallback extends AbstractJnksIotLwM2MRequestCallback<JnksIotLwM2MCancelAllRequest, Integer> {

    public JnksIotLwM2MCancelAllObserveCallback(LwM2MTelemetryLogService logService, LwM2mClient client) {
        super(logService, client);
    }

    @Override
    public void onSuccess(JnksIotLwM2MCancelAllRequest request, Integer canceledSubscriptionsCount) {
        log.trace("[{}] Cancel of all observations was successful: {}", client.getEndpoint(),  canceledSubscriptionsCount);
        logService.log(client, String.format("[%s]: Cancel of all observations was successful. Result: [%s]", LOG_LWM2M_INFO, canceledSubscriptionsCount));
    }

}
