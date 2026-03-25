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
package org.thingsboard.server.transport.tcp.service;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.data.DeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.TcpDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.TcpDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.TcpWireAuthenticationMode;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.transport.DeviceDeletedEvent;
import org.thingsboard.server.common.transport.DeviceUpdatedEvent;
import org.thingsboard.server.common.transport.TransportDeviceProfileCache;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.queue.util.AfterStartUp;
import org.thingsboard.server.queue.util.TbTcpTransportComponent;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
/**
 * SERVER + 无线上鉴权时的源 IP → 设备映射（仍使用 Core 访问令牌在传输层静默注册会话）。
 */
@TbTcpTransportComponent
@Service
@RequiredArgsConstructor
@Slf4j
public class TcpSourceBindingService {
    private final TcpProtoTransportEntityService protoEntityService;
    private final TransportDeviceProfileCache deviceProfileCache;
    private final ConcurrentHashMap<String, DeviceId> sourceHostToDeviceId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<DeviceId, String> deviceIdToNormalizedHost = new ConcurrentHashMap<>();
    @AfterStartUp(order = AfterStartUp.REGULAR_SERVICE)
    public void loadAll() {
        log.info("Loading TCP source-host bindings for NONE wire auth");
        int page = 0;
        int pageSize = 512;
        boolean hasNext;
        do {
            TransportProtos.GetTcpDevicesResponseMsg response = protoEntityService.getTcpDevicesIds(page, pageSize);
            for (String id : response.getIdsList()) {
                DeviceId deviceId = new DeviceId(UUID.fromString(id));
                Device device = protoEntityService.getDeviceById(deviceId);
                if (device != null) {
                    upsert(device);
                }
            }
            hasNext = response.getHasNextPage();
            page++;
        } while (hasNext);
        log.info("TCP source bindings loaded: {} entries", sourceHostToDeviceId.size());
    }
    @EventListener(DeviceUpdatedEvent.class)
    public void onDeviceUpdated(DeviceUpdatedEvent event) {
        Device device = event.getDevice();
        remove(device.getId());
        upsert(device);
    }
    @EventListener(DeviceDeletedEvent.class)
    public void onDeviceDeleted(DeviceDeletedEvent event) {
        remove(event.getDeviceId());
    }
    public Optional<DeviceId> findDeviceIdForRemoteAddress(SocketAddress remote) {
        if (!(remote instanceof InetSocketAddress)) {
            return Optional.empty();
        }
        InetSocketAddress isa = (InetSocketAddress) remote;
        String key = isa.getAddress().getHostAddress();
        DeviceId id = sourceHostToDeviceId.get(key);
        return Optional.ofNullable(id);
    }
    private void remove(DeviceId deviceId) {
        String host = deviceIdToNormalizedHost.remove(deviceId);
        if (host != null) {
            sourceHostToDeviceId.remove(host, deviceId);
        }
    }
    private void upsert(Device device) {
        if (device == null || device.getDeviceData() == null || device.getDeviceData().getTransportConfiguration() == null) {
            return;
        }
        DeviceTransportConfiguration tc = device.getDeviceData().getTransportConfiguration();
        if (tc.getType() != DeviceTransportType.TCP || !(tc instanceof TcpDeviceTransportConfiguration)) {
            return;
        }
        TcpDeviceTransportConfiguration dt = (TcpDeviceTransportConfiguration) tc;
        if (StringUtils.isBlank(dt.getSourceHost())) {
            return;
        }
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return;
        }
        if (!(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration)) {
            return;
        }
        TcpDeviceProfileTransportConfiguration ptc = (TcpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration();
        if (ptc.getTcpWireAuthenticationMode() != TcpWireAuthenticationMode.NONE) {
            return;
        }
        String normalized;
        try {
            normalized = InetAddress.getByName(dt.getSourceHost().trim()).getHostAddress();
        } catch (UnknownHostException e) {
            log.warn("[{}] Invalid sourceHost {}", device.getId(), dt.getSourceHost());
            return;
        }
        DeviceId existing = sourceHostToDeviceId.put(normalized, device.getId());
        if (existing != null && !existing.equals(device.getId())) {
            log.warn("Duplicate TCP sourceHost {} for devices {} and {} — last wins", normalized, existing, device.getId());
        }
        deviceIdToNormalizedHost.put(device.getId(), normalized);
    }
}