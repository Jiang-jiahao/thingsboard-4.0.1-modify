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
package org.thingsboard.server.transport.udp.service;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.data.DeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.UdpDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.UdpEffectiveServerBindPort;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.device.profile.UdpDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.UdpWireAuthenticationMode;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.transport.DeviceDeletedEvent;
import org.thingsboard.server.common.transport.DeviceProfileUpdatedEvent;
import org.thingsboard.server.common.transport.DeviceUpdatedEvent;
import org.thingsboard.server.common.transport.TransportDeviceProfileCache;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.queue.util.AfterStartUp;
import org.thingsboard.server.queue.util.TbUdpTransportComponent;
import org.thingsboard.server.transport.udp.UdpTransportService;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
/**
 * SERVER 模式下专用监听端口：优先设备档案 {@link UdpDeviceProfileTransportConfiguration#getUdpProfileServerBindPort()}，
 * 否则设备传输 {@link UdpDeviceTransportConfiguration#getServerBindPort()}（兼容旧数据）。同一端口可对应多台设备，但必须共用同一设备配置文件；
 * 无线上鉴权 NONE 且多台共享端口时须靠互异的 {@link UdpDeviceTransportConfiguration#getSourceHost()} 区分；
 * {@link UdpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID} 且多台共享端口时须靠互异的 {@link UdpDeviceTransportConfiguration#getUdpWireAuthPayloadDeviceId()} 区分。
 */
@TbUdpTransportComponent
@Service
@RequiredArgsConstructor
@Slf4j
public class UdpDedicatedListenPortService {
    private final UdpProtoTransportEntityService protoEntityService;
    private final TransportDeviceProfileCache deviceProfileCache;
    private final UdpTransportService udpTransportService;
    /**
     * 专用端口 → 绑定到该端口的设备集合（同端口须同 {@link Device#getDeviceProfileId()}，由 Core 校验）。
     */
    private final ConcurrentHashMap<Integer, Set<DeviceId>> listenPortToDeviceIds = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<DeviceId, Integer> deviceIdToListenPort = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, Integer> profileIdToListenPort = new ConcurrentHashMap<>();

    @AfterStartUp(order = AfterStartUp.REGULAR_SERVICE)
    public void loadAll() {
        log.info("Loading UDP dedicated listen port bindings");
        loadProfileListenPorts();
        int page = 0;
        int pageSize = 512;
        boolean hasNext;
        do {
            TransportProtos.GetUdpDevicesResponseMsg response = protoEntityService.getUdpDevicesIds(page, pageSize);
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
        log.info("UDP dedicated listen ports loaded: {} port(s)", listenPortToDeviceIds.size());
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

    @EventListener(DeviceProfileUpdatedEvent.class)
    public void onDeviceProfileUpdated(DeviceProfileUpdatedEvent event) {
        DeviceProfile p = event.getDeviceProfile();
        refreshProfileListenPort(p);
        if (p == null || p.getTransportType() != DeviceTransportType.UDP) {
            syncPortsWithTransport();
            return;
        }
        UUID profileUuid = p.getId().getId();
        for (DeviceId did : new HashSet<>(deviceIdToListenPort.keySet())) {
            Device d = protoEntityService.getDeviceById(did);
            if (d != null && d.getDeviceProfileId().getId().equals(profileUuid)) {
                remove(did);
            }
        }
        int page = 0;
        int pageSize = 512;
        boolean hasNext;
        do {
            TransportProtos.GetUdpDevicesResponseMsg response = protoEntityService.getUdpDevicesIds(page, pageSize);
            for (String id : response.getIdsList()) {
                DeviceId did = new DeviceId(UUID.fromString(id));
                Device d = protoEntityService.getDeviceById(did);
                if (d != null && d.getDeviceProfileId().getId().equals(profileUuid)) {
                    upsert(d);
                }
            }
            hasNext = response.getHasNextPage();
            page++;
        } while (hasNext);
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
     * 仅用于 {@link UdpWireAuthenticationMode#NONE} 的静默鉴权：TOKEN 模式返回 empty（由首帧 token 区分设备）。
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
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration)) {
            return Optional.empty();
        }
        UdpDeviceProfileTransportConfiguration ptc = (UdpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration();
        if (ptc.getUdpWireAuthenticationMode() != UdpWireAuthenticationMode.NONE) {
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

    /**
     * {@link org.thingsboard.server.common.data.device.profile.UdpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID}：
     * 本地监听端口 + 负载中的协议设备 ID → ThingsBoard 设备（同端口多设备时须在设备传输配置中配置互异的 ID）。
     */
    public Optional<DeviceId> findDeviceIdByListenPortAndProtocolDeviceId(int localPort, String protocolDeviceId) {
        if (StringUtils.isBlank(protocolDeviceId)) {
            return Optional.empty();
        }
        String needle = protocolDeviceId.trim();
        Set<DeviceId> ids = listenPortToDeviceIds.get(localPort);
        if (ids == null || ids.isEmpty()) {
            return Optional.empty();
        }
        DeviceId matched = null;
        for (DeviceId id : ids) {
            Device device = protoEntityService.getDeviceById(id);
            if (device == null || device.getDeviceData() == null
                    || !(device.getDeviceData().getTransportConfiguration() instanceof UdpDeviceTransportConfiguration dt)) {
                continue;
            }
            if (StringUtils.isBlank(dt.getUdpWireAuthPayloadDeviceId())) {
                continue;
            }
            if (needle.equals(dt.getUdpWireAuthPayloadDeviceId().trim())) {
                if (matched != null && !matched.equals(id)) {
                    log.warn("Ambiguous TCP protocol device id [{}] on local port {} (multiple device records match)", needle, localPort);
                    return Optional.empty();
                }
                matched = id;
            }
        }
        return Optional.ofNullable(matched);
    }
    public Set<Integer> collectDedicatedPorts() {
        return Collections.unmodifiableSet(listenPortToDeviceIds.keySet());
    }

    private void loadProfileListenPorts() {
        profileIdToListenPort.clear();
        int page = 0;
        int pageSize = 512;
        boolean hasNext;
        do {
            TransportProtos.GetUdpProfilesResponseMsg response = protoEntityService.getUdpProfileIds(page, pageSize);
            for (String id : response.getIdsList()) {
                DeviceProfile profile = deviceProfileCache.get(new DeviceProfileId(UUID.fromString(id)));
                refreshProfileListenPort(profile);
            }
            hasNext = response.getHasNextPage();
            page++;
        } while (hasNext);
        log.info("UDP profile listen ports loaded: {} port(s)", profileIdToListenPort.size());
    }

    private void refreshProfileListenPort(DeviceProfile profile) {
        if (profile == null || profile.getId() == null) {
            return;
        }
        UUID profileUuid = profile.getId().getId();
        profileIdToListenPort.remove(profileUuid);
        if (profile.getTransportType() != DeviceTransportType.UDP) {
            return;
        }
        Integer port = UdpEffectiveServerBindPort.resolveProfileServerListenPort(profile);
        if (port != null) {
            profileIdToListenPort.put(profileUuid, port);
        }
    }

    private Set<Integer> collectAllListenPorts() {
        Set<Integer> ports = new HashSet<>(listenPortToDeviceIds.keySet());
        ports.addAll(profileIdToListenPort.values());
        return ports;
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
        if (tc.getType() != DeviceTransportType.UDP || !(tc instanceof UdpDeviceTransportConfiguration)) {
            return;
        }
        UdpDeviceTransportConfiguration dt = (UdpDeviceTransportConfiguration) tc;
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return;
        }
        if (!(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration)) {
            return;
        }
        UdpDeviceProfileTransportConfiguration ptc = (UdpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration();
        if (ptc.getUdpProfileServerBindPort() == null) {
            return;
        }
        Integer bindPort = UdpEffectiveServerBindPort.resolve(profile, dt);
        if (bindPort == null) {
            return;
        }
        if (bindPort < 1 || bindPort > 65535) {
            log.warn("[{}] Ignoring invalid effective UDP listen port {}", device.getId(), bindPort);
            return;
        }
        listenPortToDeviceIds.computeIfAbsent(bindPort, k -> ConcurrentHashMap.newKeySet()).add(device.getId());
        deviceIdToListenPort.put(device.getId(), bindPort);
    }
    private boolean sourceHostMatchesIfConfigured(Device device, SocketAddress remote) {
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        UdpDeviceTransportConfiguration dt = (UdpDeviceTransportConfiguration) device.getDeviceData().getTransportConfiguration();
        if (StringUtils.isBlank(dt.getSourceHost())) {
            Integer effPort = UdpEffectiveServerBindPort.resolve(profile, dt);
            if (effPort == null) {
                return true;
            }
            Set<DeviceId> onPort = listenPortToDeviceIds.get(effPort);
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
        udpTransportService.syncDedicatedPorts(collectAllListenPorts());
    }
}