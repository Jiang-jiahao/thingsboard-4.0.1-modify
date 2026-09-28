package com.jnks.iot.server.transport.udp.service;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.AfterStartUp;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.device.data.UdpDeviceTransportConfiguration;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.transport.DeviceDeletedEvent;
import com.jnks.iot.server.common.transport.DeviceUpdatedEvent;
import com.jnks.iot.server.gen.transport.TransportProtos;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 设备配置里指定的 UDP <strong>固定下行地址</strong>（{@code udpDownlinkHost} + {@code udpDownlinkPort}）。
 * <p>
 * 默认下行是"回发到设备最近一次上报的源地址"，对"设备从临时/ NAT 端口上报、但固定监听某个端口收指令"
 * 这类设备不成立；这些设备在设备连接配置里填固定下行地址后，RPC / 共享属性等下发就发到该地址。
 * <p>
 * 与 {@code UdpListenPortRegistry} 同一套模式：启动时全量加载（分页拉 UDP 设备）+ 设备事件增量更新，
 * 下发路径只读内存映射，不产生 RPC。
 */
@Service
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.udp.enabled:true}'=='true'")
@RequiredArgsConstructor
@Slf4j
public class UdpDownlinkAddressRegistry {

    private final UdpProtoTransportEntityService protoEntityService;

    /** 设备 → 固定下行地址；只有配置了 udpDownlinkHost/Port 的设备才有条目。 */
    private final Map<DeviceId, InetSocketAddress> downlinkByDevice = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "udp-downlink-addr");
        t.setDaemon(true);
        return t;
    });

    @AfterStartUp(order = AfterStartUp.AFTER_TRANSPORT_SERVICE)
    public void loadAll() {
        executor.execute(this::reloadAll);
    }

    @EventListener(DeviceUpdatedEvent.class)
    public void onDeviceUpdated(DeviceUpdatedEvent event) {
        executor.execute(() -> upsert(event.getDevice()));
    }

    @EventListener(DeviceDeletedEvent.class)
    public void onDeviceDeleted(DeviceDeletedEvent event) {
        executor.execute(() -> downlinkByDevice.remove(event.getDeviceId()));
    }

    /**
     * 命中则返回配置的固定下行地址；未配置（或设备未知）返回 {@code null}，调用方回落到会话记录的上报地址。
     */
    public InetSocketAddress resolve(DeviceId deviceId) {
        return deviceId == null ? null : downlinkByDevice.get(deviceId);
    }

    private void reloadAll() {
        try {
            Map<DeviceId, InetSocketAddress> loaded = new HashMap<>();
            int page = 0;
            int pageSize = 512;
            boolean hasNext;
            do {
                TransportProtos.GetUdpDevicesResponseMsg response = protoEntityService.getUdpDevicesIds(page, pageSize);
                for (String id : response.getIdsList()) {
                    try {
                        putIfConfigured(loaded, protoEntityService.getDeviceById(new DeviceId(UUID.fromString(id))));
                    } catch (Exception e) {
                        // 列举后被删掉的设备：只跳过这一个
                        log.warn("Failed to resolve UDP device [{}] while loading downlink addresses: {}", id, e.getMessage());
                    }
                }
                hasNext = response.getHasNextPage();
                page++;
            } while (hasNext);
            downlinkByDevice.clear();
            downlinkByDevice.putAll(loaded);
            log.info("UDP fixed downlink addresses loaded: {}", downlinkByDevice.size());
        } catch (Exception e) {
            log.warn("Failed to load UDP fixed downlink addresses", e);
        }
    }

    private void upsert(Device device) {
        if (device == null || device.getId() == null) {
            return;
        }
        Map<DeviceId, InetSocketAddress> one = new HashMap<>(1);
        putIfConfigured(one, device);
        downlinkByDevice.remove(device.getId());
        InetSocketAddress addr = one.get(device.getId());
        if (addr != null) {
            downlinkByDevice.put(device.getId(), addr);
        }
    }

    private void putIfConfigured(Map<DeviceId, InetSocketAddress> target, Device device) {
        if (device == null || device.getId() == null || device.getDeviceData() == null
                || !(device.getDeviceData().getTransportConfiguration() instanceof UdpDeviceTransportConfiguration cfg)) {
            return;
        }
        if (StringUtils.isBlank(cfg.getUdpDownlinkHost()) || cfg.getUdpDownlinkPort() == null) {
            return;
        }
        try {
            target.put(device.getId(), new InetSocketAddress(
                    InetAddress.getByName(cfg.getUdpDownlinkHost().trim()), cfg.getUdpDownlinkPort()));
        } catch (UnknownHostException e) {
            log.warn("[{}] Invalid UDP downlink host [{}]: {}", device.getId(), cfg.getUdpDownlinkHost(), e.getMessage());
        }
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }
}
