/**
 * Copyright 漏 2016-2025 The Thingsboard Authors
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
package org.thingsboard.server.service.queue.processing;

import jakarta.annotation.PostConstruct;
import org.springframework.context.ApplicationEventPublisher;
import org.thingsboard.server.actors.ActorSystemContext;
import org.thingsboard.server.dao.tenant.TbTenantProfileCache;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.queue.discovery.event.PartitionChangeEvent;
import org.thingsboard.server.queue.util.AfterStartUp;
import org.thingsboard.server.service.apiusage.TbApiUsageStateService;
import org.thingsboard.server.service.cf.CalculatedFieldCache;
import org.thingsboard.server.service.profile.TbAssetProfileCache;
import org.thingsboard.server.service.profile.TbDeviceProfileCache;
import org.thingsboard.server.service.security.auth.jwt.settings.JwtSettingsService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractPartitionBasedConsumerService<N extends com.google.protobuf.GeneratedMessageV3> extends AbstractConsumerService<N> {

    private final Lock startupLock = new ReentrantLock();
    private volatile boolean started = false;
    private List<PartitionChangeEvent> pendingEvents = new ArrayList<>();

    public AbstractPartitionBasedConsumerService(ActorSystemContext actorContext,
                                                 TbTenantProfileCache tenantProfileCache,
                                                 TbDeviceProfileCache deviceProfileCache,
                                                 TbAssetProfileCache assetProfileCache,
                                                 CalculatedFieldCache calculatedFieldCache,
                                                 TbApiUsageStateService apiUsageStateService,
                                                 PartitionService partitionService,
                                                 ApplicationEventPublisher eventPublisher,
                                                 Optional<JwtSettingsService> jwtSettingsService) {
        super(actorContext, tenantProfileCache, deviceProfileCache, assetProfileCache, calculatedFieldCache, apiUsageStateService, partitionService, eventPublisher, jwtSettingsService);
    }

    @PostConstruct
    public void init() {
        super.init(getPrefix());
    }

    /**
     * 绯荤粺鍚姩瀹屾垚鍚庡鐞嗭紙AfterStartUp闃舵锛�
     *
     * 鎵ц娴佺▼锛�
     * 1. 璋冪敤鐖剁被鍚姩閫昏緫
     * 2. 瑙﹀彂瀛愮被鑷畾涔夊惎鍔ㄩ€昏緫(onStartUp)
     * 3. 瀹夊叏澶勭悊缂撳啿鐨勫垎鍖轰簨浠�
     * 4. 鏍囪鏈嶅姟宸插惎鍔�
     */
    @AfterStartUp(order = AfterStartUp.REGULAR_SERVICE)
    @Override
    public void afterStartUp() {
        super.afterStartUp();
        onStartUp();
        startupLock.lock();
        try {
            for (PartitionChangeEvent partitionChangeEvent : pendingEvents) {
                log.info("Handling partition change event: {}", partitionChangeEvent);
                try {
                    onPartitionChangeEvent(partitionChangeEvent);
                } catch (Throwable t) {
                    log.error("Failed to handle partition change event: {}", partitionChangeEvent, t);
                }
            }
            started = true;
            pendingEvents = null;
        } finally {
            startupLock.unlock();
        }
    }

    /**
     * 鍒嗗尯鍙樻洿浜嬩欢澶勭悊锛堥噸鍐欑埗绫伙級
     *
     * 澶勭悊绛栫暐锛�
     * - 鑻ユ湇鍔℃湭鍚姩锛氫簨浠跺瓨鍏ョ紦鍐插尯
     * - 鑻ユ湇鍔″凡鍚姩锛氱珛鍗冲鎵樼粰瀛愮被澶勭悊
     */
    @Override
    protected void onTbApplicationEvent(PartitionChangeEvent event) {
        log.debug("Received partition change event: {}", event);
        if (!started) {
            startupLock.lock();
            try {
                if (!started) {
                    log.debug("App not started yet, storing event for later: {}", event);
                    pendingEvents.add(event);
                    return;
                }
            } finally {
                startupLock.unlock();
            }
        }
        log.info("Handling partition change event: {}", event);
        onPartitionChangeEvent(event);
    }

    protected abstract void onStartUp();

    protected abstract void onPartitionChangeEvent(PartitionChangeEvent event);

    protected abstract String getPrefix();

}
