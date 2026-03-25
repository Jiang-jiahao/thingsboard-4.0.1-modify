/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required in writing, software distributed under the License is distributed on an "AS IS" BASIS,
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
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.thingsboard.server.common.data.device.profile.TcpTransportConnectMode;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.transport.DeviceDeletedEvent;
import org.thingsboard.server.common.transport.DeviceUpdatedEvent;
import org.thingsboard.server.common.transport.TransportDeviceProfileCache;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.queue.util.AfterStartUp;
import org.thingsboard.server.queue.util.TbTcpTransportComponent;
import org.thingsboard.server.transport.tcp.TcpTransportService;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
/**
 * SERVER 模式下 {@link TcpDeviceTransportConfiguration#getServerBindPort()}：同一端口可对应多台设备，但必须共用同一设备配置文件；
 * 无线上鉴权 NONE 且多台共享端口时须靠互异的 {@link TcpDeviceTransportConfiguration#getSourceHost()} 区分。
 */
@TbTcpTransportComponent
@Service
@RequiredArgsConstructor
@Slf4j
public class TcpDedicatedListenPortService {
    private final TcpProtoTransportEntityService protoEntityService;
    private final TransportDeviceProfileCache deviceProfileCache;
    private final TcpTransportService tcpTransportService;
    /**
     * 专用端口 → 绑定到该端口的设备集合（同端口须同 {@link Device#getDeviceProfileId()}，由 Core 校验）。
     */
    private final ConcurrentHashMap<Integer, Set<DeviceId>> listenPortToDeviceIds = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<DeviceId, Integer> deviceIdToListenPort = new ConcurrentHashMap<>();
    @AfterStartUp(order = AfterStartUp.REGULAR_SERVICE)
    public void loadAll() {
        log.info("Loading TCP dedicated listen port bindings");
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
        log.info("TCP dedicated listen ports loaded: {} port(s)", listenPortToDeviceIds.size());
        syncPortsWithTransport();
    }
    @EventListener(DeviceUpdatedEvent.class)
    public void onDeviceUpdated(DeviceUpdatedEvent event) {
        Device device = event.getDevice();
        remove(device.getId());
        upsert(device);
        syncPortsWithTransport();
    }
    @EventListener(DeviceDeletedEvent.class)
    public void onDeviceDeleted(DeviceDeletedEvent event) {
        remove(event.getDeviceId());
        syncPortsWithTransport();
    }
    /**
     * 任意一台绑定到该本地端口的设备 id（同端口同 profile，用于 pipeline 配置）。
     */
    public Optional<DeviceId> findAnyDeviceIdForLocalPort(int localPort) {
        Set<DeviceId> ids = listenPortToDeviceIds.get(localPort);
        if (ids == null || ids.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(ids.iterator().next());
    }
    /**
     * 仅用于 {@link TcpWireAuthenticationMode#NONE} 的静默鉴权：TOKEN 模式返回 empty（由首帧 token 区分设备）。
     */
    public Optional<DeviceId> findDeviceIdForDedicatedPortNoneSilentAuth(SocketAddress local, SocketAddress remote) {
        if (!(local instanceof InetSocketAddress)) {
            return Optional.empty();
        }
        int port = ((InetSocketAddress) local).getPort();
        Set<DeviceId> ids = listenPortToDeviceIds.get(port);
        if (ids == null || ids.isEmpty()) {
            return Optional.empty();
        }
        Device any = protoEntityService.getDeviceById(ids.iterator().next());
        if (any == null) {
            return Optional.empty();
        }
        DeviceProfile profile = deviceProfileCache.get(any.getDeviceProfileId());
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration)) {
            return Optional.empty();
        }
        TcpDeviceProfileTransportConfiguration ptc = (TcpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration();
        if (ptc.getTcpWireAuthenticationMode() != TcpWireAuthenticationMode.NONE) {
            return Optional.empty();
        }
        if (ids.size() == 1) {
            DeviceId id = ids.iterator().next();
            Device device = protoEntityService.getDeviceById(id);
            if (device == null) {
                return Optional.empty();
            }
            return sourceHostMatchesIfConfigured(device, remote) ? Optional.of(id) : Optional.empty();
        }
        for (DeviceId id : ids) {
            Device device = protoEntityService.getDeviceById(id);
            if (device != null && sourceHostMatchesIfConfigured(device, remote)) {
                return Optional.of(id);
            }
        }
        return Optional.empty();
    }
    public Set<Integer> collectDedicatedPorts() {
        return Collections.unmodifiableSet(listenPortToDeviceIds.keySet());
    }
    private void remove(DeviceId deviceId) {
        Integer p = deviceIdToListenPort.remove(deviceId);
        if (p == null) {
            return;
        }
        Set<DeviceId> set = listenPortToDeviceIds.get(p);
        if (set != null) {
            set.remove(deviceId);
            if (set.isEmpty()) {
                listenPortToDeviceIds.remove(p);
            }
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
        if (dt.getServerBindPort() == null) {
            return;
        }
        int bindPort = dt.getServerBindPort();
        if (bindPort < 1 || bindPort > 65535) {
            log.warn("[{}] Ignoring invalid serverBindPort {}", device.getId(), bindPort);
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
        if (ptc.getTcpTransportConnectMode() != TcpTransportConnectMode.SERVER) {
            return;
        }
        listenPortToDeviceIds.computeIfAbsent(bindPort, k -> ConcurrentHashMap.newKeySet()).add(device.getId());
        deviceIdToListenPort.put(device.getId(), bindPort);
    }
    private boolean sourceHostMatchesIfConfigured(Device device, SocketAddress remote) {
        TcpDeviceTransportConfiguration dt = (TcpDeviceTransportConfiguration) device.getDeviceData().getTransportConfiguration();
        if (StringUtils.isBlank(dt.getSourceHost())) {
            Set<DeviceId> onPort = listenPortToDeviceIds.get(dt.getServerBindPort());
            if (onPort != null && onPort.size() > 1) {
                return false;
            }
            return true;
        }
        if (!(remote instanceof InetSocketAddress)) {
            return false;
        }
        try {
            String expected = InetAddress.getByName(dt.getSourceHost().trim()).getHostAddress();
            String actual = ((InetSocketAddress) remote).getAddress().getHostAddress();
            return expected.equals(actual);
        } catch (UnknownHostException e) {
            log.warn("[{}] Invalid sourceHost {}", device.getId(), dt.getSourceHost());
            return false;
        }
    }

    private void syncPortsWithTransport() {
        tcpTransportService.syncDedicatedPorts(Set.copyOf(listenPortToDeviceIds.keySet()));
    }
}