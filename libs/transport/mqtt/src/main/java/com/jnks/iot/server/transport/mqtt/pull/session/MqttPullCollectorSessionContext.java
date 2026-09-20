package com.jnks.iot.server.transport.mqtt.pull.session;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.device.data.MqttPullDeviceTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.MqttPullDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.transport.SessionMsgListener;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionInfoProto;
import com.jnks.iot.mqtt.MqttClient;
import com.jnks.iot.server.transport.mqtt.pull.MqttPullTransportContext;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;

@Data
@Builder
public class MqttPullCollectorSessionContext {

    private TenantId tenantId;
    private Device device;
    private DeviceProfile deviceProfile;
    private String token;
    private SessionInfoProto sessionInfo;
    private MqttPullDeviceProfileTransportConfiguration profileTransportConfiguration;
    private MqttPullDeviceTransportConfiguration deviceTransportConfiguration;
    private MqttPullTransportContext transportContext;
    private MqttClient mqttClient;
    private SessionMsgListener rpcSessionListener;
    private ScheduledFuture<?> reconnectTask;
    private ScheduledFuture<?> activityHeartbeatTask;
    @Builder.Default
    private volatile boolean brokerLinkActive = false;
    @Builder.Default
    private volatile boolean destroyed = false;
    @Builder.Default
    private final Map<String, ConcurrentLinkedQueue<PendingMqttPullRpc>> pendingRpcByResponseTopic = new ConcurrentHashMap<>();
    @Builder.Default
    private final Set<String> rpcResponseSubscriptions = ConcurrentHashMap.newKeySet();

    public DeviceId getDeviceId() {
        return device.getId();
    }

    public void markDestroyed() {
        this.destroyed = true;
    }

    /**
     * Broker 断开或换新客户端后必须清空，否则会跳过对响应主题的重新订阅。
     */
    public void resetRpcState() {
        pendingRpcByResponseTopic.clear();
        rpcResponseSubscriptions.clear();
    }

    public void cancelActivityHeartbeat() {
        if (activityHeartbeatTask != null) {
            activityHeartbeatTask.cancel(false);
            activityHeartbeatTask = null;
        }
    }

    public void close() {
        markDestroyed();
        resetRpcState();
        if (reconnectTask != null) {
            reconnectTask.cancel(false);
            reconnectTask = null;
        }
        cancelActivityHeartbeat();
        if (mqttClient != null) {
            try {
                mqttClient.disconnect();
            } catch (Exception ignored) {
            }
            mqttClient = null;
        }
    }
}
