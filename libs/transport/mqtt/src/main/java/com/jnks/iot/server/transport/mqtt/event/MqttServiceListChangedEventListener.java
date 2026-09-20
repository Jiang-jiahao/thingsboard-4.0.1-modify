package com.jnks.iot.server.transport.mqtt.event;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.queue.discovery.TbApplicationEventListener;
import com.jnks.iot.server.queue.discovery.event.ServiceListChangedEvent;
import com.jnks.iot.server.transport.mqtt.MqttTransportBalancingService;

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
