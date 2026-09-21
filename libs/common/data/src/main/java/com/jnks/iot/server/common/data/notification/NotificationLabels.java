package com.jnks.iot.server.common.data.notification;

import com.jnks.iot.server.common.data.ApiFeature;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.alarm.AlarmSeverity;
import com.jnks.iot.server.common.data.audit.ActionType;

import java.util.Map;

/**
 * 通知正文里那些「运行时注入值」的中文标签。
 *
 * 这些值原样来自 Java 枚举/常量（如 actionType.name().toLowerCase() = "added"、
 * EntityType.getNormalName() = "Device"），模板翻成中文后会读成「Device 已added」这种中英混排。
 *
 * **为什么在这里做映射而不改枚举本身**：EntityType.getNormalName() 同时被
 * JnksIotOriginatorTypeSwitchNode 当作输出连接名用（规则链数据里存的就是 "Device"/"Asset"
 * 这些英文名），改了它会让已有规则链接不上线；ActionType 等也在审计日志里出现。
 * 所以只在通知这一层加映射，枚举与 API 输出保持原样。
 *
 * 链接里用的仍是英文值（模板里写成 ${entityType:lowerCase}），中文标签只用于显示。
 */
public class NotificationLabels {

    private static final Map<String, String> ALARM_ACTIONS = Map.of(
            "created", "已创建",
            "severity changed", "严重程度已变更",
            "acknowledged", "已确认",
            "cleared", "已清除",
            "deleted", "已删除",
            "assigned", "已指派",
            "unassigned", "已取消指派",
            "added", "已添加",
            "updated", "已更新"
    );

    private static final Map<String, String> DEVICE_ACTIVITY_EVENTS = Map.of(
            "active", "处于活动状态",
            "inactive", "处于非活动状态"
    );

    private static final Map<String, String> LIFECYCLE_ACTIONS = Map.of(
            "init", "初始化",
            "start", "启动",
            "update", "更新",
            "stop", "停止",
            "destroy", "销毁"
    );

    private static final Map<String, String> LIFECYCLE_EVENTS = Map.of(
            "init", "已初始化",
            "started", "已启动",
            "updated", "已更新",
            "stopped", "已停止",
            "destroyed", "已销毁"
    );

    private static final Map<String, String> API_USAGE_STATUSES = Map.of(
            "warning", "预警",
            "disabled", "已禁用"
    );

    public static String entityType(EntityType type) {
        if (type == null) {
            return null;
        }
        return switch (type) {
            case TENANT -> "租户";
            case CUSTOMER -> "客户";
            case USER -> "用户";
            case DASHBOARD -> "仪表板";
            case ASSET -> "资产";
            case DEVICE -> "设备";
            case ALARM -> "告警";
            case RULE_CHAIN -> "规则链";
            case RULE_NODE -> "规则节点";
            case ENTITY_VIEW -> "实体视图";
            case WIDGETS_BUNDLE -> "部件包";
            case WIDGET_TYPE -> "部件";
            case NOTIFICATION_TEMPLATE -> "通知模板";
            case NOTIFICATION_RULE -> "通知规则";
            case QUEUE_STATS -> "队列统计";
            case OAUTH2_CLIENT -> "OAuth2 客户端";
            case DOMAIN -> "域名";
            case MOBILE_APP -> "移动应用";
            default -> type.getNormalName();
        };
    }

    public static String actionType(ActionType actionType) {
        if (actionType == null) {
            return null;
        }
        return switch (actionType) {
            case ADDED -> "添加";
            case DELETED -> "删除";
            case UPDATED -> "更新";
            case ATTRIBUTES_UPDATED -> "更新属性";
            case ATTRIBUTES_DELETED -> "删除属性";
            case TIMESERIES_UPDATED -> "更新时序数据";
            case TIMESERIES_DELETED -> "删除时序数据";
            case RPC_CALL -> "调用 RPC";
            case CREDENTIALS_UPDATED -> "更新凭据";
            case ASSIGNED_TO_CUSTOMER -> "分配给客户";
            case UNASSIGNED_FROM_CUSTOMER -> "取消客户分配";
            case ACTIVATED -> "激活";
            case SUSPENDED -> "暂停";
            case CREDENTIALS_READ -> "读取凭据";
            case ATTRIBUTES_READ -> "读取属性";
            case RELATION_ADD_OR_UPDATE -> "更新关联";
            case RELATION_DELETED -> "删除关联";
            case RELATIONS_DELETED -> "删除所有关联";
            case REST_API_RULE_ENGINE_CALL -> "由 REST API 调用规则引擎";
            case ALARM_ACK -> "确认告警";
            case ALARM_CLEAR -> "清除告警";
            case ALARM_DELETE -> "删除告警";
            case ALARM_ASSIGNED -> "指派告警";
            case ALARM_UNASSIGNED -> "取消指派告警";
            case LOGIN -> "登录";
            case LOGOUT -> "注销";
            case LOCKOUT -> "锁定";
            case ASSIGNED_FROM_TENANT -> "从租户分配";
            case ASSIGNED_TO_TENANT -> "分配给租户";
            case PROVISION_SUCCESS -> "配网成功";
            case PROVISION_FAILURE -> "配网失败";
            case ADDED_COMMENT -> "添加评论";
            case UPDATED_COMMENT -> "更新评论";
            case DELETED_COMMENT -> "删除评论";
            case SMS_SENT -> "短信已发送";
        };
    }

    public static String alarmSeverity(AlarmSeverity severity) {
        if (severity == null) {
            return null;
        }
        return switch (severity) {
            case CRITICAL -> "严重";
            case MAJOR -> "主要";
            case MINOR -> "次要";
            case WARNING -> "警告";
            case INDETERMINATE -> "不确定";
        };
    }

    /** 告警动作，取值来自 AlarmTriggerProcessor 等处的英文字面量 */
    public static String alarmAction(String action) {
        return action == null ? null : ALARM_ACTIONS.getOrDefault(action, action);
    }

    /** 设备活动状态，取值来自 DeviceActivityTriggerProcessor（"active"/"inactive"） */
    public static String deviceActivityEvent(String eventType) {
        return eventType == null ? null : DEVICE_ACTIVITY_EVENTS.getOrDefault(eventType, eventType);
    }

    /** 规则链/节点生命周期动作：start/update/stop 等 */
    public static String lifecycleAction(String action) {
        return action == null ? null : LIFECYCLE_ACTIONS.getOrDefault(action, action);
    }

    /** 规则链/节点生命周期事件：started/stopped/updated 等 */
    public static String lifecycleEvent(String eventType) {
        return eventType == null ? null : LIFECYCLE_EVENTS.getOrDefault(eventType, eventType);
    }

    /** API 用量状态：warning/disabled */
    public static String apiUsageStatus(String status) {
        return status == null ? null : API_USAGE_STATUSES.getOrDefault(status, status);
    }

    /** API 功能名（原始标签是 "Device API"/"Telemetry persistence" 这类英文） */
    public static String apiFeature(ApiFeature feature) {
        if (feature == null) {
            return null;
        }
        return switch (feature) {
            case TRANSPORT -> "设备 API";
            case DB -> "遥测持久化";
            case RE -> "规则引擎执行";
            case JS -> "JavaScript 函数执行";
            case TBEL -> "TBEL 函数执行";
            case EMAIL -> "邮件";
            case SMS -> "短信";
            case ALARM -> "告警";
        };
    }

}
