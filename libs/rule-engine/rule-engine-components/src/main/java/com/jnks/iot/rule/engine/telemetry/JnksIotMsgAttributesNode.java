package com.jnks.iot.rule.engine.telemetry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.DonAsynchron;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.AttributesSaveRequest;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.telemetry.settings.AttributesProcessingSettings;
import com.jnks.iot.server.common.adaptor.JsonConverter;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.kv.KvEntry;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.jnks.iot.rule.engine.telemetry.settings.AttributesProcessingSettings.Advanced;
import static com.jnks.iot.rule.engine.telemetry.settings.AttributesProcessingSettings.Deduplicate;
import static com.jnks.iot.rule.engine.telemetry.settings.AttributesProcessingSettings.OnEveryMessage;
import static com.jnks.iot.rule.engine.telemetry.settings.AttributesProcessingSettings.WebSocketsOnly;
import static com.jnks.iot.server.common.data.DataConstants.NOTIFY_DEVICE_METADATA_KEY;
import static com.jnks.iot.server.common.data.DataConstants.SCOPE;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.POST_ATTRIBUTES_REQUEST;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "保存属性",
        configClazz = JnksIotMsgAttributesNodeConfiguration.class,
        version = 3,
        nodeDescription = """
                按配置的作用域和处理策略保存属性数据。
                """,
        nodeDetails = """
                节点执行三项<strong>动作：</strong>
                <ul>
                  <li><strong>属性：</strong>把属性数据写入数据库。</li>
                  <li><strong>WebSockets：</strong>通知 WebSockets 订阅方属性数据已更新。</li>
                  <li><strong>计算字段：</strong>通知计算字段属性数据已更新。</li>
                </ul>
                
                每项<em>动作</em>都有三种<strong>处理策略</strong>：
                <ul>
                  <li><strong>每条消息都执行：</strong>对每条消息都执行该动作。</li>
                  <li><strong>去重：</strong>在可配置的时间间隔内，只对同一来源方的首条消息执行该动作。</li>
                  <li><strong>跳过：</strong>从不执行该动作。</li>
                </ul>
                
                <strong>处理策略</strong>通过<em>处理设置</em>配置，支持两种模式：
                <ul>
                  <li><strong>基础</strong>
                    <ul>
                      <li><strong>每条消息都执行：</strong>对所有动作应用「每条消息都执行」策略。</li>
                      <li><strong>去重：</strong>对所有动作应用「去重」策略（可指定时间间隔）。</li>
                      <li><strong>仅 WebSockets：</strong>除 WebSocket 通知外，其余动作应用「跳过」策略，WebSocket 通知则应用「每条消息都执行」策略。</li>
                    </ul>
                  </li>
                  <li><strong>高级：</strong>为每项动作单独配置策略。</li>
                </ul>
                
                该节点支持三种属性作用域：<strong>客户端属性</strong>、<strong>共享属性</strong>和<strong>服务端属性</strong>。
                默认作用域可在节点配置里设置，也可以在消息元数据里指定合法的 <code>scope</code> 属性来覆盖。
                <br><br>
                此外：
                <ul>
                  <li>启用<b>仅在属性值变化时保存</b>后，规则节点会比较收到的属性值与当前已存值，两者相同时跳过保存。</li>
                  <li>启用<b>发送属性更新通知</b>后，规则节点会把 <code>SHARED_SCOPE</code> 和 <code>SERVER_SCOPE</code> 属性更新的 <b>Attributes Updated</b> 事件投递到名为 <code>Main</code> 的队列。</li>
                  <li>启用<b>强制通知设备</b>后，无论元数据里的 <code>notifyDevice</code> 属性为何值，规则节点都会把 <code>SHARED_SCOPE</code> 属性更新通知给设备。</li>
                </ul>
                
                该节点期望的消息类型是 <code>POST_ATTRIBUTES_REQUEST</code>。
                <br><br>
                输出连接：<code>Success</code>、<code>Failure</code>。
                """,
        configDirective = "jnksIotActionNodeAttributesConfig",
        icon = "file_upload"
)
public class JnksIotMsgAttributesNode implements JnksIotNode {

    static final String NOTIFY_DEVICE_KEY = "notifyDevice";
    static final String SEND_ATTRIBUTES_UPDATED_NOTIFICATION_KEY = "sendAttributesUpdatedNotification";
    static final String UPDATE_ATTRIBUTES_ONLY_ON_VALUE_CHANGE_KEY = "updateAttributesOnlyOnValueChange";

    private JnksIotMsgAttributesNodeConfiguration config;

    private AttributesProcessingSettings processingSettings;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        config = JnksIotNodeUtils.convert(configuration, JnksIotMsgAttributesNodeConfiguration.class);
        processingSettings = config.getProcessingSettings();
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        if (!msg.isTypeOf(POST_ATTRIBUTES_REQUEST)) {
            ctx.tellFailure(msg, new IllegalArgumentException("Unsupported msg type: " + msg.getType()));
            return;
        }
        String src = msg.getData();
        List<AttributeKvEntry> newAttributes = new ArrayList<>(JsonConverter.convertToAttributes(JsonParser.parseString(src)));
        if (newAttributes.isEmpty()) {
            ctx.tellSuccess(msg);
            return;
        }

        AttributesSaveRequest.Strategy strategy = determineSaveStrategy(msg.getMetaDataTs(), msg.getOriginator().getId());

        // short-circuit
        if (!strategy.saveAttributes() && !strategy.sendWsUpdate() && !strategy.processCalculatedFields()) {
            ctx.tellSuccess(msg);
            return;
        }

        AttributeScope scope = getScope(msg.getMetaData().getValue(SCOPE));
        boolean sendAttributesUpdateNotification = checkSendNotification(scope);

        if (!config.isUpdateAttributesOnlyOnValueChange()) {
            saveAttr(newAttributes, ctx, msg, scope, sendAttributesUpdateNotification, strategy);
            return;
        }

        List<String> keys = newAttributes.stream().map(KvEntry::getKey).collect(Collectors.toList());
        ListenableFuture<List<AttributeKvEntry>> findFuture = ctx.getAttributesService().find(ctx.getTenantId(), msg.getOriginator(), scope, keys);

        DonAsynchron.withCallback(findFuture,
                currentAttributes -> {
                    List<AttributeKvEntry> attributesChanged = filterChangedAttr(currentAttributes, newAttributes);
                    saveAttr(attributesChanged, ctx, msg, scope, sendAttributesUpdateNotification, strategy);
                },
                throwable -> ctx.tellFailure(msg, throwable),
                MoreExecutors.directExecutor());
    }

    private AttributesSaveRequest.Strategy determineSaveStrategy(long ts, UUID originatorUuid) {
        if (processingSettings instanceof OnEveryMessage) {
            return AttributesSaveRequest.Strategy.PROCESS_ALL;
        }
        if (processingSettings instanceof WebSocketsOnly) {
            return AttributesSaveRequest.Strategy.WS_ONLY;
        }
        if (processingSettings instanceof Deduplicate deduplicate) {
            boolean isFirstMsgInInterval = deduplicate.getProcessingStrategy().shouldProcess(ts, originatorUuid);
            return isFirstMsgInInterval ? AttributesSaveRequest.Strategy.PROCESS_ALL : AttributesSaveRequest.Strategy.SKIP_ALL;
        }
        if (processingSettings instanceof Advanced advanced) {
            return new AttributesSaveRequest.Strategy(
                    advanced.attributes().shouldProcess(ts, originatorUuid),
                    advanced.webSockets().shouldProcess(ts, originatorUuid),
                    advanced.calculatedFields().shouldProcess(ts, originatorUuid)
            );
        }
        // should not happen
        throw new IllegalArgumentException("Unknown processing settings type: " + processingSettings.getClass().getSimpleName());
    }

    private void saveAttr(
            List<AttributeKvEntry> attributes,
            JnksIotContext ctx,
            JnksIotMsg msg,
            AttributeScope scope,
            boolean sendAttributesUpdateNotification,
            AttributesSaveRequest.Strategy strategy
    ) {
        if (attributes.isEmpty()) {
            ctx.tellSuccess(msg);
            return;
        }
        FutureCallback<Void> callback = sendAttributesUpdateNotification ?
                new AttributesUpdateNodeCallback(ctx, msg, scope.name(), attributes) :
                new TelemetryNodeCallback(ctx, msg);
        ctx.getTelemetryService().saveAttributes(AttributesSaveRequest.builder()
                .tenantId(ctx.getTenantId())
                .entityId(msg.getOriginator())
                .scope(scope)
                .entries(attributes)
                .notifyDevice(config.isNotifyDevice() || checkNotifyDeviceMdValue(msg.getMetaData().getValue(NOTIFY_DEVICE_METADATA_KEY)))
                .strategy(strategy)
                .previousCalculatedFieldIds(msg.getPreviousCalculatedFieldIds())
                .jnksIotMsgId(msg.getId())
                .jnksIotMsgType(msg.getInternalType())
                .callback(callback)
                .build());
    }

    private List<AttributeKvEntry> filterChangedAttr(List<AttributeKvEntry> currentAttributes, List<AttributeKvEntry> newAttributes) {
        if (currentAttributes == null || currentAttributes.isEmpty()) {
            return newAttributes;
        }

        Map<String, AttributeKvEntry> currentAttrMap = currentAttributes.stream()
                .collect(Collectors.toMap(AttributeKvEntry::getKey, Function.identity(), (existing, replacement) -> existing));

        return newAttributes.stream()
                .filter(item -> {
                    AttributeKvEntry cacheAttr = currentAttrMap.get(item.getKey());
                    return cacheAttr == null
                            || !Objects.equals(item.getValue(), cacheAttr.getValue()) //JSON and String can be equals by value, but different by type
                            || !Objects.equals(item.getDataType(), cacheAttr.getDataType());
                })
                .collect(Collectors.toList());
    }

    private boolean checkSendNotification(AttributeScope scope) {
        return config.isSendAttributesUpdatedNotification() && AttributeScope.CLIENT_SCOPE != scope;
    }

    private boolean checkNotifyDeviceMdValue(String notifyDeviceMdValue) {
        // Check for empty string for backward-compatibility. A while ago node always notified devices.
        return StringUtils.isEmpty(notifyDeviceMdValue) || Boolean.parseBoolean(notifyDeviceMdValue);
    }

    private AttributeScope getScope(String mdScopeValue) {
        if (StringUtils.isNotEmpty(mdScopeValue)) {
            return AttributeScope.valueOf(mdScopeValue);
        }
        return AttributeScope.valueOf(config.getScope());
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        boolean hasChanges = false;
        switch (fromVersion) {
            case 0:
                if (!oldConfiguration.has(UPDATE_ATTRIBUTES_ONLY_ON_VALUE_CHANGE_KEY)) {
                    hasChanges = true;
                    ((ObjectNode) oldConfiguration).put(UPDATE_ATTRIBUTES_ONLY_ON_VALUE_CHANGE_KEY, false);
                }
            case 1:
                // update notifyDevice. set true if null or property doesn't exist for backward-compatibility.
                hasChanges = fixEscapedBooleanConfigParameter(oldConfiguration, NOTIFY_DEVICE_KEY, hasChanges, true);
                // update sendAttributesUpdatedNotification.
                hasChanges = fixEscapedBooleanConfigParameter(oldConfiguration, SEND_ATTRIBUTES_UPDATED_NOTIFICATION_KEY, hasChanges, false);
                // update updateAttributesOnlyOnValueChange.
                hasChanges = fixEscapedBooleanConfigParameter(oldConfiguration, UPDATE_ATTRIBUTES_ONLY_ON_VALUE_CHANGE_KEY, hasChanges, true);
            case 2:
                hasChanges = true;
                ((ObjectNode) oldConfiguration).set("processingSettings", JacksonUtil.valueToTree(new OnEveryMessage()));
                break;
            default:
                break;
        }
        return new JnksIotPair<>(hasChanges, oldConfiguration);
    }

    private boolean fixEscapedBooleanConfigParameter(JsonNode oldConfiguration, String boolKey, boolean hasChanges, boolean valueIfNull) {
        if (oldConfiguration.hasNonNull(boolKey)) {
            var value = oldConfiguration.get(boolKey);
            if (value.isTextual()) {
                hasChanges = true;
                ((ObjectNode) oldConfiguration)
                        .put(boolKey, value.asBoolean(valueIfNull));
            }
        } else {
            hasChanges = true;
            ((ObjectNode) oldConfiguration)
                    .put(boolKey, valueIfNull);
        }
        return hasChanges;
    }

}
