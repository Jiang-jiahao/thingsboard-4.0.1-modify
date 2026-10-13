package com.jnks.iot.server.transport.tcp.service;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.AfterStartUp;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.device.profile.TcpDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.TcpWireAuthenticationMode;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.transport.DeviceDeletedEvent;
import com.jnks.iot.server.common.transport.DeviceProfileUpdatedEvent;
import com.jnks.iot.server.common.transport.DeviceUpdatedEvent;
import com.jnks.iot.server.common.transport.TransportDeviceProfileCache;
import com.jnks.iot.server.gen.transport.TransportProtos;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 共享监听端口下鉴权前并不知道设备档案；延迟鉴权（{@code DEFERRED_PAYLOAD_DEVICE_ID}）
 * 靠**档案配置的 JSON 字段名**把首帧对齐到档案：
 * 帧里出现某个已配置的延迟鉴权键时，就用该档案及其鉴权模式走延迟鉴权流程。
 * <p>
 * 原先这一职责由"专用监听端口 → 档案"的映射承担，共享端口模型下改由本目录承担。
 */
@Service
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.tcp.enabled:true}'=='true'")
@RequiredArgsConstructor
@Slf4j
public class TcpDeferredAuthCatalog {

    public record Match(DeviceProfile profile, String key, String value) {
    }

    private final TcpProtoTransportEntityService protoEntityService;
    private final TransportDeviceProfileCache deviceProfileCache;
    /**
     * 键名 → 该键名下所有延迟鉴权档案。键名**不必租户内唯一**：同键名可被多个档案使用，
     * 匹配时再按"本会话监听端口"落回唯一档案（端口与档案一一对应）。
     */
    private final Map<String, Set<DeviceProfileId>> deferredKeyToProfiles = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "tcp-deferred-auth-catalog");
        t.setDaemon(true);
        return t;
    });

    @AfterStartUp(order = AfterStartUp.AFTER_TRANSPORT_SERVICE)
    public void loadAll() {
        executor.execute(this::reloadAll);
    }

    @EventListener(DeviceUpdatedEvent.class)
    public void onDeviceUpdated(DeviceUpdatedEvent event) {
        executor.execute(() -> upsertDevice(event.getDevice()));
    }

    @EventListener(DeviceDeletedEvent.class)
    public void onDeviceDeleted(DeviceDeletedEvent event) {
        executor.execute(this::reloadAll);
    }

    @EventListener(DeviceProfileUpdatedEvent.class)
    public void onDeviceProfileUpdated(DeviceProfileUpdatedEvent event) {
        executor.execute(() -> upsertProfile(event.getDeviceProfile()));
    }

    /**
     * 按帧里出现的已配置延迟鉴权键对齐档案；多个键同时命中时取第一个并告警。
     * <p>
     * 键名可能被多个档案共用，因此**必须**按本会话监听端口收敛到唯一档案（端口与档案一一对应）。
     */
    public Optional<Match> match(JsonObject frame, int localPort) {
        if (frame == null || frame.size() == 0 || deferredKeyToProfiles.isEmpty()) {
            return Optional.empty();
        }
        ArrayList<Match> matches = new ArrayList<>(2);
        for (Map.Entry<String, Set<DeviceProfileId>> entry : deferredKeyToProfiles.entrySet()) {
            JsonElement el = frame.get(entry.getKey());
            if (el == null || !el.isJsonPrimitive()) {
                continue;
            }
            String value = el.getAsString();
            if (StringUtils.isBlank(value)) {
                continue;
            }
            DeviceProfile profile = profileOnPort(entry.getValue(), localPort);
            if (profile == null) {
                continue;
            }
            matches.add(new Match(profile, entry.getKey(), value));
            if (matches.size() > 1) {
                break;
            }
        }
        if (matches.size() > 1) {
            log.warn("Deferred payload wire auth: multiple configured keys matched ({}); using [{}]",
                    matches.stream().map(Match::key).toList(), matches.get(0).key());
        }
        return matches.isEmpty() ? Optional.empty() : Optional.of(matches.get(0));
    }

    /** 该键名下、落在本监听端口上的延迟鉴权档案（端口与档案一一对应，故至多一个）。 */
    private DeviceProfile profileOnPort(Set<DeviceProfileId> profileIds, int localPort) {
        for (DeviceProfileId profileId : profileIds) {
            DeviceProfile profile = deviceProfileCache.get(profileId);
            if (profile == null || profile.getProfileData() == null
                    || !(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration ptc)) {
                continue;
            }
            if (ptc.getTcpWireAuthenticationMode() != TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
                continue;
            }
            Integer port = ptc.getTcpProfileServerBindPort();
            if (port != null && port == localPort) {
                return profile;
            }
        }
        return null;
    }

    private void reloadAll() {
        try {
            Map<String, Set<DeviceProfileId>> loaded = new ConcurrentHashMap<>();
            int page = 0;
            int pageSize = 512;
            boolean hasNext;
            do {
                TransportProtos.GetTcpDevicesResponseMsg response = protoEntityService.getTcpDevicesIds(page, pageSize);
                for (String id : response.getIdsList()) {
                    Device device = protoEntityService.getDeviceById(new DeviceId(UUID.fromString(id)));
                    putIfDeferred(loaded, device);
                }
                hasNext = response.getHasNextPage();
                page++;
            } while (hasNext);
            // 只增不删：扫描结果依赖档案缓存，冷启动/缓存未热时会漏掉设备。早先的"clear + putAll"
            // 会让一次不完整的扫描把已热的映射清空，该实例上所有设备号鉴权瞬间失效（设备连不上、要重试到缓存热起来）。
            // 档案被删或改成其它模式时，match() 会重新校验档案并跳过，因此残留条目无害。
            loaded.forEach((key, ids) ->
                    deferredKeyToProfiles.computeIfAbsent(key, __ -> ConcurrentHashMap.newKeySet()).addAll(ids));
            log.info("TCP deferred payload auth keys loaded: {}", deferredKeyToProfiles.keySet());
        } catch (Exception e) {
            log.warn("Failed to load TCP deferred payload auth keys", e);
        }
    }

    private void upsertDevice(Device device) {
        if (device == null || device.getDeviceProfileId() == null) {
            return;
        }
        upsertProfile(deviceProfileCache.get(device.getDeviceProfileId()));
    }

    private void upsertProfile(DeviceProfile profile) {
        if (profile == null || profile.getId() == null) {
            return;
        }
        deferredKeyToProfiles.values().forEach(ids -> ids.remove(profile.getId()));
        putIfDeferred(deferredKeyToProfiles, profile);
    }

    private void putIfDeferred(Map<String, Set<DeviceProfileId>> target, Device device) {
        if (device == null || device.getDeviceProfileId() == null) {
            return;
        }
        putIfDeferred(target, deviceProfileCache.get(device.getDeviceProfileId()));
    }

    private void putIfDeferred(Map<String, Set<DeviceProfileId>> target, DeviceProfile profile) {
        if (profile == null || profile.getId() == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration ptc)) {
            return;
        }
        if (ptc.getTcpWireAuthenticationMode() != TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            return;
        }
        String key = ptc.getTcpDeferredWireAuthTokenJsonKey();
        if (StringUtils.isBlank(key)) {
            return;
        }
        target.computeIfAbsent(key.trim(), __ -> ConcurrentHashMap.newKeySet()).add(profile.getId());
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }
}
