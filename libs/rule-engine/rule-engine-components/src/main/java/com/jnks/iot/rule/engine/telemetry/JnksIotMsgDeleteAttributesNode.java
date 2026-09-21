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
        name = "delete attributes",
        configClazz = JnksIotMsgDeleteAttributesNodeConfiguration.class,
        nodeDescription = "Delete attributes for Message Originator.",
        nodeDetails = "Attempt to remove attributes by selected keys. If msg originator doesn't have an attribute with " +
                " a key selected in the configuration, it will be ignored. If delete operation is completed successfully, " +
                " rule node will send the \"Attributes Deleted\" event to the root chain of the message originator and " +
                " send the incoming message via <b>Success</b> chain, otherwise, <b>Failure</b> chain is used.",
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
