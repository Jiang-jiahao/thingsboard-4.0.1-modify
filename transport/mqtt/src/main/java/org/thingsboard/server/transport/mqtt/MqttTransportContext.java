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
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.transport.TransportContext;
import org.thingsboard.server.common.transport.TransportTenantProfileCache;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.transport.mqtt.adaptors.JsonMqttAdaptor;
import org.thingsboard.server.transport.mqtt.adaptors.ProtoMqttAdaptor;
import org.thingsboard.server.transport.mqtt.gateway.GatewayMetricsService;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ashvayka on 04.10.18.
 */
@Slf4j
@Component
@TbMqttTransportComponent
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
    private final Map<DeviceId, ScheduledFuture<?>> pendingDisconnectInactivity = new ConcurrentHashMap<>();

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
        ScheduledFuture<?> pending = pendingDisconnectInactivity.remove(deviceId);
        if (pending != null) {
            pending.cancel(false);
        }
    }

    public void scheduleDisconnectInactivity(TransportProtos.SessionInfoProto sessionInfo) {
        if (sessionInfo == null) {
            return;
        }
        scheduleDisconnectInactivity(
                new TenantId(new UUID(sessionInfo.getTenantIdMSB(), sessionInfo.getTenantIdLSB())),
                new DeviceId(new UUID(sessionInfo.getDeviceIdMSB(), sessionInfo.getDeviceIdLSB())));
    }

    public void scheduleDisconnectInactivity(TenantId tenantId, DeviceId deviceId) {
        if (tenantId == null || deviceId == null) {
            return;
        }
        if (disconnectInactivityDelayMs <= 0) {
            cancelDisconnectInactivity(deviceId);
            transportService.reportDeviceInactivity(tenantId, deviceId);
            return;
        }
        ScheduledFuture<?>[] holder = new ScheduledFuture<?>[1];
        holder[0] = getScheduler().schedule(() -> {
            try {
                if (pendingDisconnectInactivity.remove(deviceId, holder[0])) {
                    log.debug("[{}] MQTT server session disconnected, reporting device inactivity", deviceId);
                    transportService.reportDeviceInactivity(tenantId, deviceId);
                }
            } catch (Exception e) {
                log.warn("[{}] Failed to report MQTT server disconnect inactivity", deviceId, e);
            }
        }, disconnectInactivityDelayMs, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> previous = pendingDisconnectInactivity.put(deviceId, holder[0]);
        if (previous != null) {
            previous.cancel(false);
        }
    }

}
