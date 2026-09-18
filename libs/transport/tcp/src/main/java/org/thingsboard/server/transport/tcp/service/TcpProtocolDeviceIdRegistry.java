package org.thingsboard.server.transport.tcp.service;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.AfterStartUp;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.data.TcpDeviceTransportConfiguration;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.transport.DeviceDeletedEvent;
import org.thingsboard.server.common.transport.DeviceUpdatedEvent;
import org.thingsboard.server.gen.transport.TransportProtos;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@code DEFERRED_PAYLOAD_DEVICE_ID} 模式下，用设备上报的"协议设备 ID"
 * （设备传输配置 {@code tcpWireAuthPayloadDeviceId}）定位 ThingsBoard 设备。
 * <p>
 * 共享监听端口下端口不再参与消歧，因此该标识必须在部署内唯一：同一标识被多台设备占用时只保留先出现的那个并打 ERROR。
 */
@Service
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.tcp.enabled:true}'=='true'")
@RequiredArgsConstructor
@Slf4j
public class TcpProtocolDeviceIdRegistry {

    private final TcpProtoTransportEntityService protoEntityService;
    private final Map<String, DeviceId> protocolDeviceIdToDeviceId = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "tcp-protocol-device-id-registry");
        t.setDaemon(true);
        return t;
    });

    @AfterStartUp(order = AfterStartUp.AFTER_TRANSPORT_SERVICE)
    public void loadAll() {
        executor.execute(this::reloadAll);
    }

    @EventListener(DeviceUpdatedEvent.class)
    public void onDeviceUpdated(DeviceUpdatedEvent event) {
        executor.execute(() -> {
            Device device = event.getDevice();
            if (device != null) {
                remove(device.getId());
                upsert(device);
            }
        });
    }

    @EventListener(DeviceDeletedEvent.class)
    public void onDeviceDeleted(DeviceDeletedEvent event) {
        executor.execute(() -> remove(event.getDeviceId()));
    }

    public Optional<DeviceId> findByProtocolDeviceId(String protocolDeviceId) {
        if (StringUtils.isBlank(protocolDeviceId)) {
            return Optional.empty();
        }
        return Optional.ofNullable(protocolDeviceIdToDeviceId.get(protocolDeviceId.trim()));
    }

    private void reloadAll() {
        try {
            Map<String, DeviceId> loaded = new ConcurrentHashMap<>();
            int page = 0;
            int pageSize = 512;
            boolean hasNext;
            do {
                TransportProtos.GetTcpDevicesResponseMsg response = protoEntityService.getTcpDevicesIds(page, pageSize);
                for (String id : response.getIdsList()) {
                    Device device = protoEntityService.getDeviceById(new DeviceId(UUID.fromString(id)));
                    if (device != null) {
                        upsertInto(loaded, device);
                    }
                }
                hasNext = response.getHasNextPage();
                page++;
            } while (hasNext);
            // 只增不删：扫描依赖"设备列表 + 档案缓存"的即时快照，刚建好的设备/尚未热的档案都可能漏。
            // 早先的 clear + putAll 会把已索引的设备号抹掉，导致该实例上设备号鉴权失效。
            // 设备被删后条目会残留，但解析时会因设备不存在而被拒（协议设备号在租户内唯一，不会误配到别人）。
            loaded.forEach(protocolDeviceIdToDeviceId::putIfAbsent);
            log.info("TCP protocol device ids loaded: {} entr(ies)", protocolDeviceIdToDeviceId.size());
        } catch (Exception e) {
            log.warn("Failed to load TCP protocol device ids", e);
        }
    }

    private void upsert(Device device) {
        if (device != null) {
            upsertInto(protocolDeviceIdToDeviceId, device);
        }
    }

    private void upsertInto(Map<String, DeviceId> target, Device device) {
        if (device == null || device.getId() == null || device.getDeviceData() == null
                || !(device.getDeviceData().getTransportConfiguration() instanceof TcpDeviceTransportConfiguration cfg)) {
            return;
        }
        String protocolDeviceId = cfg.getTcpWireAuthPayloadDeviceId();
        if (StringUtils.isBlank(protocolDeviceId)) {
            return;
        }
        DeviceId previous = target.putIfAbsent(protocolDeviceId.trim(), device.getId());
        if (previous != null && !previous.equals(device.getId())) {
            log.error("Protocol device id [{}] is used by more than one device ({} and {}); "
                            + "DEFERRED_PAYLOAD_DEVICE_ID requires a unique value, keeping {}",
                    protocolDeviceId, previous, device.getId(), previous);
        }
    }

    private void remove(DeviceId deviceId) {
        if (deviceId == null) {
            return;
        }
        protocolDeviceIdToDeviceId.values().removeIf(deviceId::equals);
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }
}
