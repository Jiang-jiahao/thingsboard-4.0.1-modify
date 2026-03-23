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
package org.thingsboard.server.service.notification.rule;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.thingsboard.rule.engine.api.NotificationCenter;
import org.thingsboard.server.cache.limits.RateLimitService;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.NotificationRequestId;
import org.thingsboard.server.common.data.id.NotificationRuleId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.limit.LimitedApi;
import org.thingsboard.server.common.data.notification.NotificationRequest;
import org.thingsboard.server.common.data.notification.NotificationRequestConfig;
import org.thingsboard.server.common.data.notification.NotificationRequestStatus;
import org.thingsboard.server.common.data.notification.info.NotificationInfo;
import org.thingsboard.server.common.data.notification.rule.NotificationRule;
import org.thingsboard.server.common.data.notification.rule.trigger.NotificationRuleTrigger;
import org.thingsboard.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerConfig;
import org.thingsboard.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;
import org.thingsboard.server.common.msg.notification.NotificationRuleProcessor;
import org.thingsboard.server.common.msg.plugin.ComponentLifecycleMsg;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.dao.notification.NotificationRequestService;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.queue.notification.NotificationDeduplicationService;
import org.thingsboard.server.service.executors.NotificationExecutorService;
import org.thingsboard.server.service.notification.rule.cache.NotificationRulesCache;
import org.thingsboard.server.service.notification.rule.trigger.NotificationRuleTriggerProcessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 通知规则处理器默认实现。
 */
@Service
@RequiredArgsConstructor
@Slf4j
@SuppressWarnings({"rawtypes", "unchecked"})
public class DefaultNotificationRuleProcessor implements NotificationRuleProcessor {

    private final NotificationRulesCache notificationRulesCache;
    private final NotificationRequestService notificationRequestService;
    private final NotificationDeduplicationService deduplicationService;
    private final PartitionService partitionService;
    private final RateLimitService rateLimitService;
    @Autowired
    @Lazy
    private NotificationCenter notificationCenter;
    private final NotificationExecutorService notificationExecutor;

    private final Map<NotificationRuleTriggerType, NotificationRuleTriggerProcessor> triggerProcessors = new EnumMap<>(NotificationRuleTriggerType.class);

    @Override
    public void process(NotificationRuleTrigger trigger) {
        // 获取触发器类型
        NotificationRuleTriggerType triggerType = trigger.getType();
        TenantId tenantId = triggerType.isTenantLevel() ? trigger.getTenantId() : TenantId.SYS_TENANT_ID;
        notificationExecutor.submit(() -> {
            try {
                // 根据触发器类型，获取所有相关规则
                List<NotificationRule> enabledRules = notificationRulesCache.getEnabled(tenantId, triggerType);
                if (enabledRules.isEmpty()) {
                    return;
                }
                // 规则去重过滤（防止短时间一直产生通知）
                if (trigger.deduplicate()) {
                    enabledRules = new ArrayList<>(enabledRules);
                    enabledRules.removeIf(rule -> deduplicationService.alreadyProcessed(trigger, rule));
                }
                final List<NotificationRule> rules = enabledRules;
                for (NotificationRule rule : rules) {
                    try {
                        processNotificationRule(rule, trigger);
                    } catch (Throwable e) {
                        log.error("Failed to process notification rule {} for trigger type {} with trigger object {}", rule.getId(), rule.getTriggerType(), trigger, e);
                    }
                }
            } catch (Throwable e) {
                log.error("Failed to process notification rules for trigger: {}", trigger, e);
            }
        });
    }

    /**
     * 处理单条通知规则。
     * <p>
     * 核心逻辑分支：
     * 1. 是否匹配“清除”规则？ -> 如果是，则删除该实体所有**已调度**的通知请求。
     * 2. 是否匹配“过滤”条件？ -> 如果是，则进行限流检查，然后为每个接收目标（可配置延迟）提交通知请求。
     *
     * @param rule 当前待处理的规则
     * @param trigger 传入的触发器
     */
    private void processNotificationRule(NotificationRule rule, NotificationRuleTrigger trigger) {
        NotificationRuleTriggerConfig triggerConfig = rule.getTriggerConfig();
        log.debug("Processing notification rule '{}' for trigger type {}", rule.getName(), rule.getTriggerType());

        // 检查是否为“清除”型规则（例如：设备上线后，清除掉“设备离线”的待发通知）
        if (matchesClearRule(trigger, triggerConfig)) {
            List<NotificationRequest> scheduledRequests = notificationRequestService.findNotificationRequestsByRuleIdAndOriginatorEntityIdAndStatus(
                    rule.getTenantId(), rule.getId(), trigger.getOriginatorEntityId(), NotificationRequestStatus.SCHEDULED
            );
            scheduledRequests.forEach(notificationRequest -> {
                notificationCenter.deleteNotificationRequest(rule.getTenantId(), notificationRequest.getId());
            });
            // 清除操作后，直接返回，不发送新通知
            return;
        }
        // 检查是否满足规则的触发过滤器
        if (matchesFilter(trigger, triggerConfig)) {
            if (!rateLimitService.checkRateLimit(LimitedApi.NOTIFICATION_REQUESTS_PER_RULE, rule.getTenantId(), rule.getId())) {
                log.debug("[{}] Rate limit for notification requests per rule was exceeded (rule '{}')", rule.getTenantId(), rule.getName());
                return;
            }
            // 构造通知的具体内容信息（由对应的TriggerProcessor负责）
            NotificationInfo notificationInfo = constructNotificationInfo(trigger, triggerConfig);

            // 遍历规则中配置的“目标-延迟”表，为每一组提交请求
            // 支持对同一规则的不同接收者设置不同的发送延迟（rule指定了接收信息的目标）
            rule.getRecipientsConfig().getTargetsTable().forEach((delay, targets) -> {
                submitNotificationRequest(targets, rule, trigger.getOriginatorEntityId(), notificationInfo, delay);
            });
        }
    }

    private void submitNotificationRequest(List<UUID> targets, NotificationRule rule,
                                           EntityId originatorEntityId, NotificationInfo notificationInfo, int delayInSec) {
        NotificationRequestConfig config = new NotificationRequestConfig();
        if (delayInSec > 0) {
            config.setSendingDelayInSec(delayInSec);
        }
        NotificationRequest notificationRequest = NotificationRequest.builder()
                .tenantId(rule.getTenantId())
                .targets(targets)
                .templateId(rule.getTemplateId())
                .additionalConfig(config)
                .info(notificationInfo)
                .ruleId(rule.getId())
                .originatorEntityId(originatorEntityId)
                .build();

        try {
            log.debug("Submitting notification request for rule '{}' with delay of {} sec to targets {}", rule.getName(), delayInSec, targets);
            notificationCenter.processNotificationRequest(rule.getTenantId(), notificationRequest, null);
        } catch (Exception e) {
            log.error("Failed to process notification request for tenant {} for rule {}", rule.getTenantId(), rule.getId(), e);
        }
    }

    private boolean matchesFilter(NotificationRuleTrigger trigger, NotificationRuleTriggerConfig triggerConfig) {
        return triggerProcessors.get(triggerConfig.getTriggerType()).matchesFilter(trigger, triggerConfig);
    }

    private boolean matchesClearRule(NotificationRuleTrigger trigger, NotificationRuleTriggerConfig triggerConfig) {
        return triggerProcessors.get(triggerConfig.getTriggerType()).matchesClearRule(trigger, triggerConfig);
    }

    private NotificationInfo constructNotificationInfo(NotificationRuleTrigger trigger, NotificationRuleTriggerConfig triggerConfig) {
        return triggerProcessors.get(triggerConfig.getTriggerType()).constructNotificationInfo(trigger);
    }

    @EventListener(ComponentLifecycleMsg.class)
    public void onNotificationRuleDeleted(ComponentLifecycleMsg componentLifecycleMsg) {
        if (componentLifecycleMsg.getEvent() != ComponentLifecycleEvent.DELETED ||
                componentLifecycleMsg.getEntityId().getEntityType() != EntityType.NOTIFICATION_RULE) {
            return;
        }

        TenantId tenantId = componentLifecycleMsg.getTenantId();
        NotificationRuleId notificationRuleId = (NotificationRuleId) componentLifecycleMsg.getEntityId();
        if (partitionService.isMyPartition(ServiceType.TB_CORE, tenantId, notificationRuleId)) {
            notificationExecutor.submit(() -> {
                List<NotificationRequestId> scheduledForRule = notificationRequestService.findNotificationRequestsIdsByStatusAndRuleId(tenantId, NotificationRequestStatus.SCHEDULED, notificationRuleId);
                for (NotificationRequestId notificationRequestId : scheduledForRule) {
                    notificationCenter.deleteNotificationRequest(tenantId, notificationRequestId);
                }
            });
        }
    }

    @Autowired
    public void setTriggerProcessors(Collection<NotificationRuleTriggerProcessor> processors) {
        processors.forEach(processor -> {
            triggerProcessors.put(processor.getTriggerType(), processor);
        });
    }

}
