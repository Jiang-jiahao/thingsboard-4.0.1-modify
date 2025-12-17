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
package org.thingsboard.server.queue.notification;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import org.springframework.util.ConcurrentReferenceHashMap;
import org.thingsboard.server.common.data.CacheConstants;
import org.thingsboard.server.common.data.notification.rule.NotificationRule;
import org.thingsboard.server.common.data.notification.rule.trigger.NotificationRuleTrigger;
import org.thingsboard.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;
import org.thingsboard.server.queue.util.PropertyUtils;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.springframework.util.ConcurrentReferenceHashMap.ReferenceType.SOFT;

@Service
@RequiredArgsConstructor
@Slf4j
public class DefaultNotificationDeduplicationService implements NotificationDeduplicationService {

    /**
     * 【配置存储】触发器类型 -> 去重时间窗口（毫秒）的映射。
     * 优先级：配置文件 > 触发器默认值。
     * 例如：{ALARM: 300000} 表示告警触发器的去重窗口为5分钟。
     */
    private ConcurrentMap<NotificationRuleTriggerType, Long> deduplicationDurations;

    /**
     * 【外部缓存管理器】用于集群环境下的去重状态共享。
     * 通常指向一个集中式缓存（如Redis），确保所有服务实例看到一致的去重状态。
     * `required = false` 表示如果Spring上下文中没有CacheManager Bean，则此字段为null（兼容单机部署）。
     */
    @Autowired(required = false)
    private CacheManager cacheManager;

    /**
     * 【本地内存缓存】用于快速查找，避免频繁访问外部缓存。
     * 使用ConcurrentReferenceHashMap并指定SOFT引用，允许在内存不足时被GC自动清理，防止OOM。
     * Key: 去重键（String）
     * Value: 上次处理的时间戳（Long）
     */
    private final ConcurrentMap<String, Long> localCache = new ConcurrentReferenceHashMap<>(16, SOFT);

    @Override
    public boolean alreadyProcessed(NotificationRuleTrigger trigger) {
        String deduplicationKey = trigger.getDeduplicationKey();
        return alreadyProcessed(trigger, deduplicationKey, true);
    }

    @Override
    public boolean alreadyProcessed(NotificationRuleTrigger trigger, NotificationRule rule) {
        String deduplicationKey = getDeduplicationKey(trigger, rule);
        return alreadyProcessed(trigger, deduplicationKey, false);
    }

    /**
     * 执行去重检查的通用逻辑。
     * <p>
     * 1. 生成去重键。
     * 2. 查询缓存（先本地，后外部）获取上次处理时间 `lastProcessedTs`。
     * 3. 计算时间间隔 `passed = 当前时间 - lastProcessedTs`。
     * 4. 判断：如果 `lastProcessedTs` 存在 且 `passed <= 去重时间窗口`，则返回 true（已处理）。
     * 5. 更新缓存（无论是否重复，都刷新时间戳，用于后续判断）。
     *
     * @param trigger         触发器
     * @param deduplicationKey 计算好的去重键
     * @param onlyLocalCache  是否只使用本地缓存（true：仅本地；false：本地+外部）
     * @return 是否已处理
     */
    private boolean alreadyProcessed(NotificationRuleTrigger trigger, String deduplicationKey, boolean onlyLocalCache) {
        Long lastProcessedTs = localCache.get(deduplicationKey);
        if (lastProcessedTs == null && !onlyLocalCache) {
            Cache externalCache = getExternalCache();
            if (externalCache != null) {
                lastProcessedTs = externalCache.get(deduplicationKey, Long.class);
            } else {
                log.warn("Sent notifications cache is not set up");
            }
        }

        boolean alreadyProcessed = false;
        long deduplicationDuration = getDeduplicationDuration(trigger);
        if (lastProcessedTs != null) {
            long passed = System.currentTimeMillis() - lastProcessedTs;
            log.trace("Deduplicating trigger {} by key '{}'. Deduplication duration: {} ms, passed: {} ms",
                    trigger.getType(), deduplicationKey, deduplicationDuration, passed);
            if (deduplicationDuration == 0 || passed <= deduplicationDuration) {
                alreadyProcessed = true;
            }
        }

        if (!alreadyProcessed) {
            lastProcessedTs = System.currentTimeMillis();
        }
        localCache.put(deduplicationKey, lastProcessedTs);
        if (!onlyLocalCache) {
            if (!alreadyProcessed || deduplicationDuration == 0) {
                // if lastProcessedTs is changed or if deduplicating infinitely (so that cache value not removed by ttl)
                Cache externalCache = getExternalCache();
                if (externalCache != null) {
                    externalCache.put(deduplicationKey, lastProcessedTs);
                }
            }
        }
        return alreadyProcessed;
    }

    public static String getDeduplicationKey(NotificationRuleTrigger trigger, NotificationRule rule) {
        return String.join("_", trigger.getDeduplicationKey(), rule.getDeduplicationKey());
    }

    private long getDeduplicationDuration(NotificationRuleTrigger trigger) {
        return deduplicationDurations.computeIfAbsent(trigger.getType(), triggerType -> {
            return trigger.getDefaultDeduplicationDuration();
        });
    }

    private Cache getExternalCache() {
        return Optional.ofNullable(cacheManager)
                .map(cacheManager -> cacheManager.getCache(CacheConstants.SENT_NOTIFICATIONS_CACHE))
                .orElse(null);
    }

    @Autowired
    public void setDeduplicationDurations(@Value("${notification_system.rules.deduplication_durations:}")
                                          String deduplicationDurationsStr) {
        this.deduplicationDurations = new ConcurrentHashMap<>();
        PropertyUtils.getProps(deduplicationDurationsStr).forEach((triggerType, duration) -> {
            this.deduplicationDurations.put(NotificationRuleTriggerType.valueOf(triggerType), Long.parseLong(duration));
        });
    }

}
