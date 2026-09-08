/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.transport.mqtt;

import io.netty.handler.ssl.SslHandler;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.transport.TransportContext;
import org.thingsboard.server.common.transport.TransportTenantProfileCache;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.transport.mqtt.adaptors.JsonMqttAdaptor;
import org.thingsboard.server.transport.mqtt.adaptors.ProtoMqttAdaptor;
import org.thingsboard.server.transport.mqtt.gateway.GatewayMetricsService;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ashvayka on 04.10.18.
 */
@Slf4j
@Component
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.mqtt.enabled:true}'=='true'")
public class MqttTransportContext extends TransportContext {

    @Getter
    @Autowired(required = false)
    private MqttSslHandlerProvider sslHandlerProvider;

    @Getter
    @Autowired
    private JsonMqttAdaptor jsonMqttAdaptor;

    @Getter
    @Autowired
    private ProtoMqttAdaptor protoMqttAdaptor;

    @Getter
    @Autowired
    private TransportTenantProfileCache tenantProfileCache;

    @Getter
    @Autowired
    private GatewayMetricsService gatewayMetricsService;

    @Getter
    @Value("${transport.mqtt.netty.max_payload_size}")
    private Integer maxPayloadSize;

    @Getter
    @Value("${transport.mqtt.ssl.skip_validity_check_for_client_cert:false}")
    private boolean skipValidityCheckForClientCert;

    @Getter
    @Setter
    private SslHandler sslHandler;

    @Getter
    @Value("${transport.mqtt.msg_queue_size_per_device_limit:100}")
    private int messageQueueSizePerDeviceLimit;

    @Getter
    @Value("${transport.mqtt.timeout:10000}")
    private long timeout;

    @Getter
    @Value("${transport.mqtt.disconnect_timeout:1000}")
    private long disconnectTimeout;

    /**
     * MQTT 服务端断开后延迟多久标为非活跃。0 表示立即上报。
     * 短延迟用于闪断重连，避免和立刻非活跃来回抖动。
     */
    @Getter
    @Value("${transport.mqtt.disconnect_inactivity_delay_ms:5000}")
    private long disconnectInactivityDelayMs;

    @Getter
    @Value("${transport.mqtt.proxy_enabled:false}")
    private boolean proxyEnabled;

    private final AtomicInteger connectionsCounter = new AtomicInteger();
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final Map<UUID, TransportProtos.SessionInfoProto> connectedMqttServerSessions = new ConcurrentHashMap<>();
    private final Map<DeviceId, PendingDisconnectInactivity> pendingDisconnectInactivity = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        super.init();
        transportService.createGaugeStats("openConnections", connectionsCounter);
    }

    public void channelRegistered() {
        connectionsCounter.incrementAndGet();
    }

    public void channelUnregistered() {
        connectionsCounter.decrementAndGet();
    }

    public boolean checkAddress(InetSocketAddress address) {
        return rateLimitService.checkAddress(address);
    }

    public void onAuthSuccess(InetSocketAddress address) {
        rateLimitService.onAuthSuccess(address);
    }

    public void onAuthFailure(InetSocketAddress address) {
        rateLimitService.onAuthFailure(address);
    }

    public void cancelDisconnectInactivity(DeviceId deviceId) {
        if (deviceId == null) {
            return;
        }
        PendingDisconnectInactivity pending = pendingDisconnectInactivity.remove(deviceId);
        if (pending != null) {
            pending.future().cancel(false);
        }
    }

    /**
     * 设备连上 MQTT 服务端后登记会话，并取消尚未生效的断开非活跃任务。
     */
    public void registerMqttServerSession(TransportProtos.SessionInfoProto sessionInfo) {
        if (sessionInfo == null) {
            return;
        }
        DeviceId deviceId = toDeviceId(sessionInfo);
        cancelDisconnectInactivity(deviceId);
        connectedMqttServerSessions.put(toSessionId(sessionInfo), sessionInfo);
    }

    public void scheduleDisconnectInactivity(TransportProtos.SessionInfoProto sessionInfo) {
        if (sessionInfo == null) {
            return;
        }
        connectedMqttServerSessions.remove(toSessionId(sessionInfo));
        scheduleDisconnectInactivity(toTenantId(sessionInfo), toDeviceId(sessionInfo));
    }

    public void scheduleDisconnectInactivity(TenantId tenantId, DeviceId deviceId) {
        if (tenantId == null || deviceId == null) {
            return;
        }
        if (hasOtherConnectedSession(deviceId)) {
            cancelDisconnectInactivity(deviceId);
            return;
        }
        if (shuttingDown.get() || disconnectInactivityDelayMs <= 0) {
            cancelDisconnectInactivity(deviceId);
            reportInactivity(tenantId, deviceId);
            return;
        }
        ScheduledFuture<?>[] holder = new ScheduledFuture<?>[1];
        holder[0] = getScheduler().schedule(() -> {
            try {
                PendingDisconnectInactivity current = pendingDisconnectInactivity.get(deviceId);
                if (current != null && current.future() == holder[0]
                        && pendingDisconnectInactivity.remove(deviceId, current)) {
                    log.debug("[{}] MQTT server session disconnected, reporting device inactivity", deviceId);
                    reportInactivity(tenantId, deviceId);
                }
            } catch (Exception e) {
                log.warn("[{}] Failed to report MQTT server disconnect inactivity", deviceId, e);
            }
        }, disconnectInactivityDelayMs, TimeUnit.MILLISECONDS);
        PendingDisconnectInactivity previous = pendingDisconnectInactivity.put(
                deviceId, new PendingDisconnectInactivity(tenantId, deviceId, holder[0]));
        if (previous != null) {
            previous.future().cancel(false);
        }
    }

    /**
     * 传输进程退出前立刻把仍在线或待延迟的设备标为非活跃。
     * 正常断开仍走短延迟；重启时延迟任务会随进程一起丢掉。
     */
    public void flushMqttServerDisconnectInactivity() {
        shuttingDown.set(true);
        Set<DeviceId> reported = new HashSet<>();
        for (TransportProtos.SessionInfoProto sessionInfo : connectedMqttServerSessions.values()) {
            DeviceId deviceId = toDeviceId(sessionInfo);
            if (reported.add(deviceId)) {
                reportInactivity(toTenantId(sessionInfo), deviceId);
            }
        }
        connectedMqttServerSessions.clear();
        for (PendingDisconnectInactivity pending : pendingDisconnectInactivity.values()) {
            pending.future().cancel(false);
            if (reported.add(pending.deviceId())) {
                reportInactivity(pending.tenantId(), pending.deviceId());
            }
        }
        pendingDisconnectInactivity.clear();
        if (transportService != null) {
            transportService.closeLocalSessionsAndReportInactivity();
            transportService.flushToCore();
        }
        log.info("Flushed MQTT server disconnect inactivity for {} device(s)", reported.size());
    }

    private boolean hasOtherConnectedSession(DeviceId deviceId) {
        for (TransportProtos.SessionInfoProto sessionInfo : connectedMqttServerSessions.values()) {
            if (deviceId.equals(toDeviceId(sessionInfo))) {
                return true;
            }
        }
        return false;
    }

    private void reportInactivity(TenantId tenantId, DeviceId deviceId) {
        if (tenantId == null || deviceId == null || transportService == null) {
            return;
        }
        transportService.reportDeviceInactivity(tenantId, deviceId);
    }

    private static UUID toSessionId(TransportProtos.SessionInfoProto sessionInfo) {
        return new UUID(sessionInfo.getSessionIdMSB(), sessionInfo.getSessionIdLSB());
    }

    private static TenantId toTenantId(TransportProtos.SessionInfoProto sessionInfo) {
        return TenantId.fromUUID(new UUID(sessionInfo.getTenantIdMSB(), sessionInfo.getTenantIdLSB()));
    }

    private static DeviceId toDeviceId(TransportProtos.SessionInfoProto sessionInfo) {
        return new DeviceId(new UUID(sessionInfo.getDeviceIdMSB(), sessionInfo.getDeviceIdLSB()));
    }

    private record PendingDisconnectInactivity(TenantId tenantId, DeviceId deviceId, ScheduledFuture<?> future) {
    }

}
