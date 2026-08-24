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
package org.thingsboard.server.service.session;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.thingsboard.server.cache.TbTransactionalCache;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.gen.transport.TransportProtos.DeviceSessionsCacheEntry;
import org.thingsboard.server.queue.util.TbCoreComponent;

import java.util.Collections;

/**
 * 设备传输会话缓存服务。
 * <p>
 * Core 侧缓存设备当前 Transport 会话列表，供会话查询与集群协调；底层为事务缓存。
 *
 * @see DeviceSessionCacheService
 */
@Service
@TbCoreComponent
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
