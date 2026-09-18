package org.thingsboard.server.transport.http.event;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import org.thingsboard.server.queue.discovery.TbApplicationEventListener;
import org.thingsboard.server.queue.discovery.event.ServiceListChangedEvent;
import org.thingsboard.server.transport.http.HttpTransportBalancingService;

@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.http.enabled:true}'=='true'")
@Component
@RequiredArgsConstructor
public class HttpServiceListChangedEventListener extends TbApplicationEventListener<ServiceListChangedEvent> {

    private final HttpTransportBalancingService httpTransportBalancingService;

    @Override
    protected void onTbApplicationEvent(ServiceListChangedEvent event) {
        httpTransportBalancingService.onServiceListChanged(event);
    }
}
