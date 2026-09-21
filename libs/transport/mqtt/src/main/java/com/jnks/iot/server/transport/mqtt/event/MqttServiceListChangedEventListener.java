package com.jnks.iot.server.transport.mqtt.event;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.queue.discovery.JnksIotApplicationEventListener;
import com.jnks.iot.server.queue.discovery.event.ServiceListChangedEvent;
import com.jnks.iot.server.transport.mqtt.MqttTransportBalancingService;

@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.mqtt.enabled:true}'=='true'")
@Component
@RequiredArgsConstructor
public class MqttServiceListChangedEventListener extends JnksIotApplicationEventListener<ServiceListChangedEvent> {

    private final MqttTransportBalancingService mqttTransportBalancingService;

    @Override
    protected void onJnksIotApplicationEvent(ServiceListChangedEvent event) {
        mqttTransportBalancingService.onServiceListChanged(event);
    }
}
