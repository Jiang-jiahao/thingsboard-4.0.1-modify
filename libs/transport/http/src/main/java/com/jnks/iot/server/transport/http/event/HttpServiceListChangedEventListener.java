package com.jnks.iot.server.transport.http.event;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.queue.discovery.JnksIotApplicationEventListener;
import com.jnks.iot.server.queue.discovery.event.ServiceListChangedEvent;
import com.jnks.iot.server.transport.http.HttpTransportBalancingService;

@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.http.enabled:true}'=='true'")
@Component
@RequiredArgsConstructor
public class HttpServiceListChangedEventListener extends JnksIotApplicationEventListener<ServiceListChangedEvent> {

    private final HttpTransportBalancingService httpTransportBalancingService;

    @Override
    protected void onJnksIotApplicationEvent(ServiceListChangedEvent event) {
        httpTransportBalancingService.onServiceListChanged(event);
    }
}
