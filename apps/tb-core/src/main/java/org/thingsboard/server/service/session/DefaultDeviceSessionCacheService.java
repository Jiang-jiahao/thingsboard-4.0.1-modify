package org.thingsboard.server.service.session;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.thingsboard.server.cache.TbTransactionalCache;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.gen.transport.TransportProtos.DeviceSessionsCacheEntry;
import java.util.Collections;

/**
 * 设备传输会话缓存服务。
 * <p>
 * Core 侧缓存设备当前 Transport 会话列表，供会话查询与集群协调；底层为事务缓存。
 *
 * @see DeviceSessionCacheService
 */
@Service
@Slf4j
public class DefaultDeviceSessionCacheService implements DeviceSessionCacheService {

    @Autowired
    protected TbTransactionalCache<DeviceId, DeviceSessionsCacheEntry> cache;

    /**
     * 读取会话缓存；未命中则写入空会话列表。
     */
    @Override
    public DeviceSessionsCacheEntry get(DeviceId deviceId) {
        log.debug("[{}] Fetching session data from cache", deviceId);
        return cache.getAndPutInTransaction(deviceId, () ->
                DeviceSessionsCacheEntry.newBuilder().addAllSessions(Collections.emptyList()).build(), false);
    }

    /**
     * 覆盖写入设备会话列表。
     */
    @Override
    public DeviceSessionsCacheEntry put(DeviceId deviceId, DeviceSessionsCacheEntry sessions) {
        log.debug("[{}] Pushing session data to cache: {}", deviceId, sessions);
        cache.put(deviceId, sessions);
        return sessions;
    }
}
