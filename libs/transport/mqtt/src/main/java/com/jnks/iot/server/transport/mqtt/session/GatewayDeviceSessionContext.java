package com.jnks.iot.server.transport.mqtt.session;

import lombok.ToString;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.transport.TransportService;
import com.jnks.iot.server.common.transport.auth.TransportDeviceInfo;

import java.util.concurrent.ConcurrentMap;

/**
 * Created by nickAS21 on 26.12.22
 */
@ToString(callSuper = true)
public class GatewayDeviceSessionContext extends AbstractGatewayDeviceSessionContext<GatewaySessionHandler> {

    public GatewayDeviceSessionContext(GatewaySessionHandler parent,
                                       TransportDeviceInfo deviceInfo,
                                       DeviceProfile deviceProfile,
                                       ConcurrentMap<MqttTopicMatcher, Integer> mqttQoSMap,
                                       TransportService transportService) {
        super(parent, deviceInfo, deviceProfile, mqttQoSMap, transportService);
    }

}
