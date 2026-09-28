package com.jnks.iot.server.transport.tcp.service;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.AfterStartUp;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.device.profile.TcpDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.transport.DeviceProfileDeletedEvent;
import com.jnks.iot.server.common.transport.DeviceProfileUpdatedEvent;
import com.jnks.iot.server.common.transport.TransportDeviceProfileCache;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.transport.tcp.TcpTransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 档案声明的 TCP 自定义监听端口：维护「端口 → 声明它的档案」，并驱动 {@link TcpTransportService} 动态绑定/解绑。
 * <p>
 * 所有 transport 实例都监听全部端口（SO_REUSEPORT），不做按档案分片的归属计算，因此设备连网关/LB 的任一端口
 * 都能落到任一实例；端口只服务声明它的档案——入站首帧即按该档案的分帧解码（见 {@code TcpTransportServerInitializer}）。
 * <p>
 * 端口集合 = 全局默认共享端口 ∪ 本表登记的档案端口。档案被删、改端口或改传输类型时，端口随之解绑。
 */
@Service
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.tcp.enabled:true}'=='true'")
@RequiredArgsConstructor
@Slf4j
public class TcpListenPortRegistry {

    private final TcpProtoTransportEntityService protoEntityService;
    private final TransportDeviceProfileCache deviceProfileCache;
    private final TcpTransportService tcpTransportService;

    /** 自定义监听端口 → 声明该端口的档案；默认共享端口不在此表内。 */
    private final Map<Integer, DeviceProfileId> profileByListenPort = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "tcp-listen-port");
        t.setDaemon(true);
        return t;
    });

    @AfterStartUp(order = AfterStartUp.AFTER_TRANSPORT_SERVICE)
    public void loadAll() {
        executor.execute(this::reloadAll);
    }

    @EventListener(DeviceProfileUpdatedEvent.class)
    public void onDeviceProfileUpdated(DeviceProfileUpdatedEvent event) {
        executor.execute(() -> refresh(event.getDeviceProfile()));
    }

    @EventListener(DeviceProfileDeletedEvent.class)
    public void onDeviceProfileDeleted(DeviceProfileDeletedEvent event) {
        executor.execute(() -> {
            profileByListenPort.values().removeIf(event.getDeviceProfileId()::equals);
            syncListenPorts();
        });
    }

    /**
     * 该本地端口是否由某个档案声明；命中时入站连接在鉴权前即可按该档案处理。
     */
    public Optional<DeviceProfile> profileForListenPort(int localPort) {
        DeviceProfileId deviceProfileId = profileByListenPort.get(localPort);
        if (deviceProfileId == null) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(deviceProfileCache.get(deviceProfileId));
        } catch (Exception e) {
            log.warn("Failed to resolve device profile [{}] for listen port {}: {}", deviceProfileId, localPort, e.getMessage());
            return Optional.empty();
        }
    }

    private void reloadAll() {
        try {
            Map<Integer, DeviceProfileId> loaded = new HashMap<>();
            int page = 0;
            int pageSize = 512;
            boolean hasNext;
            do {
                TransportProtos.GetTcpProfilesResponseMsg response = protoEntityService.getTcpProfileIds(page, pageSize);
                for (String id : response.getIdsList()) {
                    try {
                        registerPort(loaded, deviceProfileCache.get(new DeviceProfileId(UUID.fromString(id))));
                    } catch (Exception e) {
                        // 列举后被删掉的档案：解析失败只跳过这一个，不能让整个端口集合加载失败
                        log.warn("Failed to resolve TCP device profile [{}] while loading listen ports: {}", id, e.getMessage());
                    }
                }
                hasNext = response.getHasNextPage();
                page++;
            } while (hasNext);
            // 事件回调与本方法共用单线程执行器，因此这里直接整体替换不会与增量更新交错。
            profileByListenPort.clear();
            profileByListenPort.putAll(loaded);
            log.info("TCP custom listen ports loaded: {}", profileByListenPort.keySet());
            syncListenPorts();
        } catch (Exception e) {
            log.warn("Failed to load TCP custom listen ports", e);
        }
    }

    private void refresh(DeviceProfile profile) {
        if (profile == null || profile.getId() == null) {
            return;
        }
        profileByListenPort.values().removeIf(profile.getId()::equals);
        registerPort(profileByListenPort, profile);
        syncListenPorts();
    }

    private void registerPort(Map<Integer, DeviceProfileId> target, DeviceProfile profile) {
        if (profile == null || profile.getId() == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration ptc)) {
            return;
        }
        Integer bindPort = ptc.getTcpProfileServerBindPort();
        if (bindPort == null) {
            return;
        }
        if (bindPort < 1 || bindPort > 65535) {
            log.warn("Device profile [{}] declares invalid TCP listen port {}; ignoring", profile.getName(), bindPort);
            return;
        }
        DeviceProfileId existing = target.putIfAbsent(bindPort, profile.getId());
        if (existing != null && !existing.equals(profile.getId())) {
            log.error("TCP listen port {} is declared by more than one device profile ({} and {}); keeping {}. "
                            + "Device profile validation should have rejected the second profile.",
                    bindPort, existing, profile.getId(), existing);
        }
    }

    private void syncListenPorts() {
        try {
            tcpTransportService.syncListenPorts(profileByListenPort.keySet());
        } catch (Exception e) {
            log.error("Failed to sync TCP listen ports {}", profileByListenPort.keySet(), e);
        }
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }
}
