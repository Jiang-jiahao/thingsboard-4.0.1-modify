/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.tcp.service;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.AfterStartUp;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.profile.TcpDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.TcpWireAuthenticationMode;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.transport.DeviceDeletedEvent;
import org.thingsboard.server.common.transport.DeviceProfileUpdatedEvent;
import org.thingsboard.server.common.transport.DeviceUpdatedEvent;
import org.thingsboard.server.common.transport.TransportDeviceProfileCache;
import org.thingsboard.server.gen.transport.TransportProtos;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
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
    private final Map<String, DeviceProfileId> deferredKeyToProfile = new ConcurrentHashMap<>();
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
     */
    public Optional<Match> match(JsonObject frame) {
        if (frame == null || frame.size() == 0 || deferredKeyToProfile.isEmpty()) {
            return Optional.empty();
        }
        ArrayList<Match> matches = new ArrayList<>(2);
        for (Map.Entry<String, DeviceProfileId> entry : deferredKeyToProfile.entrySet()) {
            JsonElement el = frame.get(entry.getKey());
            if (el == null || !el.isJsonPrimitive()) {
                continue;
            }
            String value = el.getAsString();
            if (StringUtils.isBlank(value)) {
                continue;
            }
            DeviceProfile profile = deviceProfileCache.get(entry.getValue());
            if (profile == null || profile.getProfileData() == null
                    || !(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration ptc)) {
                continue;
            }
            if (ptc.getTcpWireAuthenticationMode() != TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
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

    private void reloadAll() {
        try {
            Map<String, DeviceProfileId> loaded = new ConcurrentHashMap<>();
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
            loaded.forEach(deferredKeyToProfile::put);
            log.info("TCP deferred payload auth keys loaded: {}", deferredKeyToProfile.keySet());
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
        deferredKeyToProfile.values().removeIf(profile.getId()::equals);
        putIfDeferred(deferredKeyToProfile, profile);
    }

    private void putIfDeferred(Map<String, DeviceProfileId> target, Device device) {
        if (device == null || device.getDeviceProfileId() == null) {
            return;
        }
        putIfDeferred(target, deviceProfileCache.get(device.getDeviceProfileId()));
    }

    private void putIfDeferred(Map<String, DeviceProfileId> target, DeviceProfile profile) {
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
        target.put(key.trim(), profile.getId());
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }
}
