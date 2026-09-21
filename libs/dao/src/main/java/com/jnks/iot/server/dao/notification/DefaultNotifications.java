package com.jnks.iot.server.dao.notification;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.ApiUsageStateValue;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.alarm.AlarmSearchStatus;
import com.jnks.iot.server.common.data.id.NotificationTargetId;
import com.jnks.iot.server.common.data.id.NotificationTemplateId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.limit.LimitedApi;
import com.jnks.iot.server.common.data.notification.NotificationDeliveryMethod;
import com.jnks.iot.server.common.data.notification.NotificationType;
import com.jnks.iot.server.common.data.notification.rule.DefaultNotificationRuleRecipientsConfig;
import com.jnks.iot.server.common.data.notification.rule.EscalatedNotificationRuleRecipientsConfig;
import com.jnks.iot.server.common.data.notification.rule.NotificationRule;
import com.jnks.iot.server.common.data.notification.rule.NotificationRuleConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.AlarmAssignmentNotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.AlarmCommentNotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.AlarmNotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.AlarmNotificationRuleTriggerConfig.AlarmAction;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.ApiUsageLimitNotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.DeviceActivityNotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.DeviceActivityNotificationRuleTriggerConfig.DeviceEvent;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.EntitiesLimitNotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.EntityActionNotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NewPlatformVersionNotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.RateLimitsNotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.RuleEngineComponentLifecycleEventNotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.TaskProcessingFailureNotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.template.NotificationTemplate;
import com.jnks.iot.server.common.data.notification.template.NotificationTemplateConfig;
import com.jnks.iot.server.common.data.notification.template.WebDeliveryMethodNotificationTemplate;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleEvent;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;
import static com.jnks.iot.common.util.JacksonUtil.newObjectNode;
import static com.jnks.iot.server.dao.DaoUtil.toUUIDs;

@Service
@RequiredArgsConstructor
public class DefaultNotifications {

    private static final String YELLOW_COLOR = "#F9D916";
    private static final String RED_COLOR = "#e91a1a";

    public static final DefaultNotification maintenanceWork = DefaultNotification.builder()
            .name("维护作业通知")
            .subject("基础设施维护")
            .text("维护作业计划于明天进行（UTC 7:00 - 9:00）")
            .build();

    public static final DefaultNotification entitiesLimitForSysadmin = DefaultNotification.builder()
            .name("实体数量上限预警通知（系统管理员）")
            .type(NotificationType.ENTITIES_LIMIT)
            .subject("租户 ${tenantName} 的 ${entityType} 数量即将达到上限")
            .text("${entityType} 用量：${currentCount}/${limit}（${percents}%）")
            .icon("warning").color(YELLOW_COLOR)
            .rule(DefaultRule.builder()
                    .name("实体数量上限预警（系统管理员）")
                    .triggerConfig(EntitiesLimitNotificationRuleTriggerConfig.builder()
                            .entityTypes(null).threshold(0.8f)
                            .build())
                    .description("租户的某类实体数量达到上限的 80% 时通知系统管理员")
                    .build())
            .build();
    public static final DefaultNotification entitiesLimitForTenant = entitiesLimitForSysadmin.toBuilder()
            .name("实体数量上限预警通知（租户）")
            .subject("警告：${entityType} 数量即将达到上限")
            .rule(entitiesLimitForSysadmin.getRule().toBuilder()
                    .name("实体数量上限预警")
                    .description("某类实体数量达到上限的 80% 时通知租户管理员")
                    .build())
            .build();

    public static final DefaultNotification apiFeatureWarningForSysadmin = DefaultNotification.builder()
            .name("API 功能用量预警通知（系统管理员）")
            .type(NotificationType.API_USAGE_LIMIT)
            .subject("租户 ${tenantName} 的 ${feature} 功能即将被禁用")
            .text("用量：${currentValue}，上限 ${limit}")
            .icon("warning").color(YELLOW_COLOR)
            .rule(DefaultRule.builder()
                    .name("API 功能用量预警（系统管理员）")
                    .triggerConfig(ApiUsageLimitNotificationRuleTriggerConfig.builder()
                            .apiFeatures(null)
                            .notifyOn(Set.of(ApiUsageStateValue.WARNING))
                            .build())
                    .description("租户的 API 功能用量进入预警状态时通知系统管理员")
                    .build())
            .build();
    public static final DefaultNotification apiFeatureWarningForTenant = apiFeatureWarningForSysadmin.toBuilder()
            .name("API 功能用量预警通知（租户）")
            .subject("警告：${feature} 功能即将被禁用")
            .rule(apiFeatureWarningForSysadmin.getRule().toBuilder()
                    .name("API 功能用量预警")
                    .description("API 功能用量进入预警状态时通知租户管理员")
                    .build())
            .build();
    public static final DefaultNotification apiFeatureDisabledForSysadmin = DefaultNotification.builder()
            .name("API 功能禁用通知（系统管理员）")
            .type(NotificationType.API_USAGE_LIMIT)
            .subject("租户 ${tenantName} 的 ${feature} 功能已被禁用")
            .text("已使用 ${currentValue}，上限 ${limit}")
            .icon("block").color(RED_COLOR)
            .rule(DefaultRule.builder()
                    .name("API 功能被禁用（系统管理员）")
                    .triggerConfig(ApiUsageLimitNotificationRuleTriggerConfig.builder()
                            .apiFeatures(null)
                            .notifyOn(Set.of(ApiUsageStateValue.DISABLED))
                            .build())
                    .description("租户的 API 功能被禁用时通知系统管理员")
                    .build())
            .build();
    public static final DefaultNotification apiFeatureDisabledForTenant = apiFeatureDisabledForSysadmin.toBuilder()
            .name("API 功能禁用通知（租户）")
            .subject("${feature} 功能已被禁用")
            .rule(apiFeatureDisabledForSysadmin.getRule().toBuilder()
                    .name("API 功能被禁用")
                    .description("API 功能被禁用时通知租户管理员")
                    .build())
            .build();

    public static final DefaultNotification exceededRateLimits = DefaultNotification.builder()
            .name("超出单租户限流通知（租户）")
            .type(NotificationType.RATE_LIMITS)
            .subject("已超出限流阈值")
            .text("${api} 已超出限流阈值")
            .icon("block").color(RED_COLOR)
            .rule(DefaultRule.builder()
                    .name("超出单租户限流")
                    .triggerConfig(RateLimitsNotificationRuleTriggerConfig.builder()
                            .apis(Arrays.stream(LimitedApi.values())
                                    .filter(LimitedApi::isPerTenant)
                                    .filter(api -> api.getLabel() != null)
                                    .collect(Collectors.toSet()))
                            .build())
                    .description("超出任一单租户限流时通知租户管理员")
                    .build())
            .build();
    public static final DefaultNotification exceededPerEntityRateLimits = DefaultNotification.builder()
            .name("超出单实体限流通知（租户）")
            .type(NotificationType.RATE_LIMITS)
            .subject("已超出限流阈值")
            .text("'${limitLevelEntityName}' 的 ${api} 已超出限流阈值")
            .icon("block").color(RED_COLOR)
            .rule(DefaultRule.builder()
                    .name("超出单实体限流")
                    .triggerConfig(RateLimitsNotificationRuleTriggerConfig.builder()
                            .apis(Arrays.stream(LimitedApi.values())
                                    .filter(not(LimitedApi::isPerTenant))
                                    .filter(api -> api.getLabel() != null)
                                    .collect(Collectors.toSet()))
                            .build())
                    .description("某实体的单实体限流被超出时通知租户管理员")
                    .build())
            .build();
    public static final DefaultNotification exceededRateLimitsForSysadmin = exceededRateLimits.toBuilder()
            .name("超出单租户限流通知（系统管理员）")
            .subject("租户 ${tenantName} 已超出限流阈值")
            .button("查看租户").link("/tenants/${tenantId}")
            .rule(exceededRateLimits.getRule().toBuilder()
                    .name("超出单租户限流（系统管理员）")
                    .description("租户超出任一单租户限流时通知系统管理员")
                    .build())
            .build();

    public static final DefaultNotification newPlatformVersion = DefaultNotification.builder()
            .name("平台新版本通知")
            .type(NotificationType.NEW_PLATFORM_VERSION)
            .subject("新版本 <b>${latestVersion}</b> 已发布")
            .text("当前平台版本为 ${currentVersion}")
            .button("查看发行说明").link("${latestVersionReleaseNotesUrl}")
            .rule(DefaultRule.builder()
                    .name("平台新版本")
                    .triggerConfig(new NewPlatformVersionNotificationRuleTriggerConfig())
                    .description("有新平台版本可用时通知系统管理员")
                    .build())
            .build();

    public static final DefaultNotification newAlarm = DefaultNotification.builder()
            .name("新告警通知")
            .type(NotificationType.ALARM)
            .subject("新告警 '${alarmType}'")
            .text("严重程度：${alarmSeverity}，来源：${alarmOriginatorEntityType} '${alarmOriginatorName}'")
            .icon("notifications").color(null)
            .rule(DefaultRule.builder()
                    .name("新告警")
                    .triggerConfig(AlarmNotificationRuleTriggerConfig.builder()
                            .alarmTypes(null)
                            .alarmSeverities(null)
                            .notifyOn(Set.of(AlarmAction.CREATED))
                            .build())
                    .description("产生新告警时通知租户管理员")
                    .build())
            .build();
    public static final DefaultNotification alarmUpdate = DefaultNotification.builder()
            .name("告警更新通知")
            .type(NotificationType.ALARM)
            .subject("告警 '${alarmType}' - ${action}")
            .text("严重程度：${alarmSeverity}，来源：${alarmOriginatorEntityType} '${alarmOriginatorName}'")
            .icon("notifications").color(null)
            .rule(DefaultRule.builder()
                    .name("告警更新")
                    .triggerConfig(AlarmNotificationRuleTriggerConfig.builder()
                            .alarmTypes(null)
                            .alarmSeverities(null)
                            .notifyOn(Set.of(AlarmAction.SEVERITY_CHANGED, AlarmAction.ACKNOWLEDGED, AlarmAction.CLEARED))
                            .build())
                    .description("告警被更新或清除时通知租户管理员")
                    .build())
            .build();
    public static final DefaultNotification entityAction = DefaultNotification.builder()
            .name("实体操作通知")
            .type(NotificationType.ENTITY_ACTION)
            .subject("${entityTypeLabel}已${actionType}")
            .text("${entityTypeLabel} '${entityName}' 已被用户 ${userEmail} ${actionType}")
            .icon("info").color(null)
            .button("查看${entityTypeLabel}").link("/${entityType:lowerCase}s/${entityId}")
            .rule(DefaultRule.builder()
                    .name("设备创建")
                    .triggerConfig(EntityActionNotificationRuleTriggerConfig.builder()
                            .entityTypes(Set.of(EntityType.DEVICE))
                            .created(true)
                            .updated(false)
                            .deleted(false)
                            .build())
                    .description("创建设备时通知租户管理员")
                    .build())
            .build();
    public static final DefaultNotification deviceActivity = DefaultNotification.builder()
            .name("设备活动通知")
            .type(NotificationType.DEVICE_ACTIVITY)
            .subject("设备 '${deviceName}' ${eventType}")
            .text("类型为 '${deviceType}' 的设备 '${deviceName}' 当前${eventType}")
            .icon("info").color(null)
            .button("查看设备").link("/devices/${deviceId}")
            .rule(DefaultRule.builder()
                    .name("设备活动状态变化")
                    .enabled(false)
                    .triggerConfig(DeviceActivityNotificationRuleTriggerConfig.builder()
                            .devices(null)
                            .deviceProfiles(null)
                            .notifyOn(Set.of(DeviceEvent.ACTIVE, DeviceEvent.INACTIVE))
                            .build())
                    .description("设备活动状态变化时通知租户管理员")
                    .build())
            .build();
    public static final DefaultNotification alarmComment = DefaultNotification.builder()
            .name("告警评论通知")
            .type(NotificationType.ALARM_COMMENT)
            .subject("'${alarmType}' 告警有新评论")
            .text("${userEmail} ${action}评论：${comment}")
            .icon("people").color(null)
            .rule(DefaultRule.builder()
                    .name("活动告警的评论")
                    .triggerConfig(AlarmCommentNotificationRuleTriggerConfig.builder()
                            .alarmTypes(null)
                            .alarmSeverities(null)
                            .alarmStatuses(Set.of(AlarmSearchStatus.ACTIVE))
                            .onlyUserComments(true)
                            .notifyOnCommentUpdate(false)
                            .build())
                    .description("用户在活动告警上添加评论时通知租户管理员")
                    .build())
            .build();
    public static final DefaultNotification alarmAssignment = DefaultNotification.builder()
            .name("告警指派通知")
            .type(NotificationType.ALARM_ASSIGNMENT)
            .subject("告警 '${alarmType}'（${alarmSeverity}）已指派给用户")
            .text("${userEmail} 将 ${alarmOriginatorEntityType} '${alarmOriginatorName}' 上的告警指派给 ${assigneeEmail}")
            .icon("person").color(null)
            .rule(DefaultRule.builder()
                    .name("告警指派")
                    .triggerConfig(AlarmAssignmentNotificationRuleTriggerConfig.builder()
                            .alarmTypes(null)
                            .alarmSeverities(null)
                            .alarmStatuses(null)
                            .notifyOn(Set.of(AlarmAssignmentNotificationRuleTriggerConfig.Action.ASSIGNED))
                            .build())
                    .description("告警被指派给某用户时通知该用户")
                    .build())
            .build();
    public static final DefaultNotification ruleEngineComponentLifecycleFailure = DefaultNotification.builder()
            .name("规则链/节点生命周期失败通知")
            .type(NotificationType.RULE_ENGINE_COMPONENT_LIFECYCLE_EVENT)
            .subject("规则链 '${ruleChainName}' 中${action}失败")
            .text("${componentType} '${componentName}' ${action}失败")
            .icon("warning").color(null)
            .button("查看规则链").link("/ruleChains/${ruleChainId}")
            .rule(DefaultRule.builder()
                    .name("规则节点初始化失败")
                    .triggerConfig(RuleEngineComponentLifecycleEventNotificationRuleTriggerConfig.builder()
                            .ruleChains(null)
                            .ruleChainEvents(Set.of(ComponentLifecycleEvent.STARTED, ComponentLifecycleEvent.UPDATED, ComponentLifecycleEvent.STOPPED))
                            .onlyRuleChainLifecycleFailures(true)
                            .trackRuleNodeEvents(true)
                            .ruleNodeEvents(Set.of(ComponentLifecycleEvent.STARTED, ComponentLifecycleEvent.UPDATED, ComponentLifecycleEvent.STOPPED))
                            .onlyRuleNodeLifecycleFailures(true)
                            .build())
                    .description("规则链或规则节点启动、更新、停止失败时通知租户管理员")
                    .build())
            .build();

    public static final DefaultNotification taskProcessingFailure = DefaultNotification.builder()
            .name("任务处理失败通知")
            .type(NotificationType.TASK_PROCESSING_FAILURE)
            .subject("处理 ${taskType} 失败")
            .text("为租户 ${tenantId} 处理 ${taskDescription} 失败：${error}")
            .icon("warning").color(YELLOW_COLOR)
            .rule(DefaultRule.builder()
                    .name("任务处理失败")
                    .triggerConfig(TaskProcessingFailureNotificationRuleTriggerConfig.builder().build())
                    .description("任务处理失败时通知系统管理员")
                    .build())
            .build();

    private final NotificationTemplateService templateService;
    private final NotificationRuleService ruleService;

    public final void create(TenantId tenantId, DefaultNotification defaultNotification, NotificationTargetId... targets) {
        NotificationTemplate template = defaultNotification.toTemplate();
        template.setTenantId(tenantId);
        template = templateService.saveNotificationTemplate(tenantId, template);

        if (defaultNotification.getRule() != null && targets.length > 0) {
            NotificationRule rule = defaultNotification.toRule(template.getId(), targets);
            rule.setTenantId(tenantId);
            ruleService.saveNotificationRule(tenantId, rule);
        }
    }

    @Data
    @Builder(toBuilder = true)
    public static class DefaultNotification {

        private final String name;
        private final NotificationType type;
        private final String subject;
        private final String text;
        private final String icon;
        private final String color;
        private final String button;
        private final String link;

        private final DefaultRule rule;

        public NotificationTemplate toTemplate() {
            NotificationTemplate template = new NotificationTemplate();
            template.setName(name);
            template.setNotificationType(type != null ? type : NotificationType.GENERAL);

            NotificationTemplateConfig templateConfig = new NotificationTemplateConfig();
            WebDeliveryMethodNotificationTemplate webTemplate = new WebDeliveryMethodNotificationTemplate();
            webTemplate.setSubject(subject);
            webTemplate.setBody(text);
            ObjectNode additionalConfig = newObjectNode();
            ObjectNode iconConfig = newObjectNode();
            additionalConfig.set("icon", iconConfig);
            ObjectNode buttonConfig = newObjectNode();
            additionalConfig.set("actionButtonConfig", buttonConfig);
            if (icon != null) {
                iconConfig.put("enabled", true)
                        .put("icon", icon)
                        .put("color", color != null ? color : "#757575");
            } else {
                iconConfig.put("enabled", false);
            }
            if (button != null) {
                buttonConfig.put("enabled", true)
                        .put("text", button)
                        .put("linkType", "LINK")
                        .put("link", link);
            } else {
                buttonConfig.put("enabled", false);
            }
            webTemplate.setAdditionalConfig(additionalConfig);
            webTemplate.setEnabled(true);
            templateConfig.setDeliveryMethodsTemplates(Map.of(
                    NotificationDeliveryMethod.WEB, webTemplate
            ));
            template.setConfiguration(templateConfig);
            return template;
        }

        public NotificationRule toRule(NotificationTemplateId templateId, NotificationTargetId... targets) {
            DefaultRule defaultRule = this.rule;
            NotificationRule rule = new NotificationRule();
            rule.setName(defaultRule.getName());
            rule.setEnabled(defaultRule.getEnabled() == null || defaultRule.getEnabled());
            rule.setTemplateId(templateId);
            rule.setTriggerType(defaultRule.getTriggerConfig().getTriggerType());
            rule.setTriggerConfig(defaultRule.getTriggerConfig());
            if (rule.getTriggerType() == NotificationRuleTriggerType.ALARM) {
                EscalatedNotificationRuleRecipientsConfig recipientsConfig = new EscalatedNotificationRuleRecipientsConfig();
                recipientsConfig.setTriggerType(rule.getTriggerType());
                recipientsConfig.setEscalationTable(Map.of(0, toUUIDs(List.of(targets))));
                rule.setRecipientsConfig(recipientsConfig);
            } else {
                DefaultNotificationRuleRecipientsConfig recipientsConfig = new DefaultNotificationRuleRecipientsConfig();
                recipientsConfig.setTriggerType(rule.getTriggerType());
                recipientsConfig.setTargets(toUUIDs(List.of(targets)));
                rule.setRecipientsConfig(recipientsConfig);
            }
            NotificationRuleConfig additionalConfig = new NotificationRuleConfig();
            additionalConfig.setDescription(defaultRule.getDescription());
            rule.setAdditionalConfig(additionalConfig);
            return rule;
        }

    }

    @Data
    @Builder(toBuilder = true)
    public static class DefaultRule {
        private final String name;
        private final Boolean enabled;
        private final NotificationRuleTriggerConfig triggerConfig;
        private final String description;
    }

}
