package com.jnks.iot.rule.engine.telemetry;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.AttributesDeleteRequest;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.jnks.iot.server.common.data.DataConstants.NOTIFY_DEVICE_METADATA_KEY;
import static com.jnks.iot.server.common.data.DataConstants.SCOPE;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "删除属性",
        configClazz = JnksIotMsgDeleteAttributesNodeConfiguration.class,
        nodeDescription = "删除消息来源方的属性。",
        nodeDetails = "尝试按选中的键删除属性。若消息来源方没有具有 " +
                " 配置中所选键的属性，则该键将被忽略。若删除操作成功完成， " +
                " 规则节点将向消息来源方的根链发送 \"Attributes Deleted\" 事件，并 " +
                " 通过 <b>Success</b> 链发送传入消息，否则使用 <b>Failure</b> 链。",
        configDirective = "jnksIotActionNodeDeleteAttributesConfig",
        icon = "remove_circle"
)
public class JnksIotMsgDeleteAttributesNode implements JnksIotNode {

    private JnksIotMsgDeleteAttributesNodeConfiguration config;
    private List<String> keys;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotMsgDeleteAttributesNodeConfiguration.class);
        this.keys = config.getKeys();
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException, JnksIotNodeException {
        List<String> keysToDelete = keys.stream()
                .map(keyPattern -> JnksIotNodeUtils.processPattern(keyPattern, msg))
                .distinct()
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
        if (keysToDelete.isEmpty()) {
            ctx.tellSuccess(msg);
        } else {
            AttributeScope scope = getScope(msg.getMetaData().getValue(SCOPE));
            ctx.getTelemetryService().deleteAttributes(AttributesDeleteRequest.builder()
                    .tenantId(ctx.getTenantId())
                    .entityId(msg.getOriginator())
                    .scope(scope)
                    .keys(keysToDelete)
                    .notifyDevice(checkNotifyDevice(msg.getMetaData().getValue(NOTIFY_DEVICE_METADATA_KEY), scope))
                    .previousCalculatedFieldIds(msg.getPreviousCalculatedFieldIds())
                    .jnksIotMsgId(msg.getId())
                    .jnksIotMsgType(msg.getInternalType())
                    .callback(config.isSendAttributesDeletedNotification() ?
                            new AttributesDeleteNodeCallback(ctx, msg, scope.name(), keysToDelete) :
                            new TelemetryNodeCallback(ctx, msg))
                    .build());
        }
    }

    private AttributeScope getScope(String mdScopeValue) {
        if (StringUtils.isNotEmpty(mdScopeValue)) {
            return AttributeScope.valueOf(mdScopeValue);
        }
        return AttributeScope.valueOf(config.getScope());
    }

    private boolean checkNotifyDevice(String notifyDeviceMdValue, AttributeScope scope) {
        return (AttributeScope.SHARED_SCOPE == scope) && (config.isNotifyDevice() || Boolean.parseBoolean(notifyDeviceMdValue));
    }

}
