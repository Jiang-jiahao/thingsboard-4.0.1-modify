package org.thingsboard.server.transport.mqtt.event;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import org.thingsboard.server.queue.discovery.TbApplicationEventListener;
import org.thingsboard.server.queue.discovery.event.ServiceListChangedEvent;
import org.thingsboard.server.transport.mqtt.MqttTransportBalancingService;

@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.mqtt.enabled:true}'=='true'")
@Component
@RequiredArgsConstructor
public class MqttServiceListChangedEventListener extends TbApplicationEventListener<ServiceListChangedEvent> {

    private final MqttTransportBalancingService mqttTransportBalancingService;

    @Override
    protected void onTbApplicationEvent(ServiceListChangedEvent event) {
        mqttTransportBalancingService.onServiceListChanged(event);
    }
}
