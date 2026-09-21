package com.jnks.iot.rule.engine.flow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.Optional;
import java.util.UUID;

@Slf4j
@RuleNode(
        type = ComponentType.FLOW,
        name = "子规则链",
        configClazz = JnksIotRuleChainInputNodeConfiguration.class,
        version = 1,
        nodeDescription = "将消息传输到另一个规则链",
        nodeDetails = "传入消息会被转发到目标规则链的输入节点。 " +
                "若启用「将消息转发到来源方的默认规则链」， " +
                "则目标规则链可能会根据传入消息的来源方动态解析。 " +
                "此时，配置中指定的规则链将作为备选规则链。<br><br>" +
                "输出连接：<i>目标规则链中输出节点产生的任意连接。</i>",
        configDirective = "jnksIotFlowNodeRuleChainInputConfig",
        relationTypes = {},
        ruleChainNode = true,
        customRelations = true
)
public class JnksIotRuleChainInputNode implements JnksIotNode {

    private RuleChainId ruleChainId;
    private boolean forwardMsgToDefaultRuleChain;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        JnksIotRuleChainInputNodeConfiguration config = JnksIotNodeUtils.convert(configuration, JnksIotRuleChainInputNodeConfiguration.class);
        if (config.getRuleChainId() == null) {
            throw new JnksIotNodeException("Rule chain must be set!", true);
        }
        UUID ruleChainUUID;
        try {
            ruleChainUUID = UUID.fromString(config.getRuleChainId());
        } catch (Exception e) {
            throw new JnksIotNodeException("Failed to parse rule chain id: " + config.getRuleChainId(), true);
        }
        ruleChainId = new RuleChainId(ruleChainUUID);
        ctx.checkTenantEntity(ruleChainId);
        forwardMsgToDefaultRuleChain = config.isForwardMsgToDefaultRuleChain();
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws JnksIotNodeException {
        RuleChainId targetRuleChainId = forwardMsgToDefaultRuleChain ?
                getOriginatorDefaultRuleChainId(ctx, msg).orElse(ruleChainId) : ruleChainId;
        ctx.input(msg, targetRuleChainId);
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        boolean hasChanges = false;
        switch (fromVersion) {
            case 0 -> {
                if (!oldConfiguration.has("forwardMsgToDefaultRuleChain")) {
                    hasChanges = true;
                    ((ObjectNode) oldConfiguration).put("forwardMsgToDefaultRuleChain", false);
                }
            }
            default -> {
            }
        }
        return new JnksIotPair<>(hasChanges, oldConfiguration);
    }

    private Optional<RuleChainId> getOriginatorDefaultRuleChainId(JnksIotContext ctx, JnksIotMsg msg) {
        return Optional.ofNullable(
                switch (msg.getOriginator().getEntityType()) {
                    case DEVICE ->
                            ctx.getDeviceProfileCache().get(ctx.getTenantId(), (DeviceId) msg.getOriginator()).getDefaultRuleChainId();
                    case ASSET ->
                            ctx.getAssetProfileCache().get(ctx.getTenantId(), (AssetId) msg.getOriginator()).getDefaultRuleChainId();
                    default -> null;
                });
    }
}
