package com.jnks.iot.server.actors.ruleChain;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.DebugModeUtil;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.JnksIotActorCtx;
import com.jnks.iot.server.actors.JnksIotRuleNodeUpdateException;
import com.jnks.iot.server.actors.shared.ComponentMsgProcessor;
import com.jnks.iot.server.common.data.ApiUsageRecordKey;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleState;
import com.jnks.iot.server.common.data.rule.RuleNode;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.queue.PartitionChangeMsg;
import com.jnks.iot.server.common.msg.queue.RuleNodeException;
import com.jnks.iot.server.common.msg.queue.RuleNodeInfo;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.common.stats.JnksIotApiUsageReportClient;
import com.jnks.iot.server.gen.transport.TransportProtos;

/**
 * @author Andrew Shvayka
 */
@Slf4j
public class RuleNodeActorMessageProcessor extends ComponentMsgProcessor<RuleNodeId> {

    private static final String UNKNOWN_NAME = "Unknown";
    private final String ruleChainName;
    private final JnksIotApiUsageReportClient apiUsageClient;
    private final DefaultJnksIotContext defaultCtx;
    private RuleNode ruleNode;
    private JnksIotNode jnksIotNode;
    private RuleNodeInfo info;

    RuleNodeActorMessageProcessor(TenantId tenantId, String ruleChainName,
                                  RuleNodeId ruleNodeId, ActorSystemContext systemContext, JnksIotActorCtx selfActor) {
        super(systemContext, tenantId, ruleNodeId);
        this.apiUsageClient = systemContext.getApiUsageClient();
        this.ruleChainName = ruleChainName;
        this.ruleNode = systemContext.getRuleChainService().findRuleNodeById(tenantId, entityId);
        this.defaultCtx = new DefaultJnksIotContext(systemContext, ruleChainName, new RuleNodeCtx(tenantId, selfActor, ruleNode));
        this.info = new RuleNodeInfo(ruleNodeId, ruleChainName, getName(ruleNode));
    }

    @Override
    public void start(JnksIotActorCtx context) throws Exception {
        if (isMyNodePartition()) {
            log.debug("[{}][{}] Starting", tenantId, entityId);
            jnksIotNode = initComponent(ruleNode);
            if (jnksIotNode != null) {
                state = ComponentLifecycleState.ACTIVE;
            }
        }
    }

    @Override
    public void onUpdate(JnksIotActorCtx context) throws Exception {
        RuleNode newRuleNode = systemContext.getRuleChainService().findRuleNodeById(tenantId, entityId);
        if (isMyNodePartition(newRuleNode)) {
            this.info = new RuleNodeInfo(entityId, ruleChainName, getName(newRuleNode));
            boolean restartRequired = state != ComponentLifecycleState.ACTIVE ||
                    !(ruleNode.getType().equals(newRuleNode.getType()) &&
                            ruleNode.getConfiguration().equals(newRuleNode.getConfiguration()));
            this.ruleNode = newRuleNode;
            this.defaultCtx.updateSelf(newRuleNode);
            if (restartRequired) {
                if (jnksIotNode != null) {
                    jnksIotNode.destroy();
                }
                try {
                    start(context);
                } catch (Exception e) {
                    throw new JnksIotRuleNodeUpdateException("Failed to update rule node", e);
                }
            }
        } else if (jnksIotNode != null) {
            stop(null);
            jnksIotNode = null;
        }
    }

    @Override
    public void stop(JnksIotActorCtx context) {
        log.debug("[{}][{}] Stopping", tenantId, entityId);
        if (jnksIotNode != null) {
            jnksIotNode.destroy();
            state = ComponentLifecycleState.SUSPENDED;
        }
    }

    @Override
    public void onPartitionChangeMsg(PartitionChangeMsg msg) throws Exception {
        log.debug("[{}][{}] onPartitionChangeMsg: [{}]", tenantId, entityId, msg);
        if (jnksIotNode != null) {
            if (!isMyNodePartition()) {
                stop(null);
                jnksIotNode = null;
            } else {
                jnksIotNode.onPartitionChangeMsg(defaultCtx, msg);
            }
        } else if (isMyNodePartition()) {
            start(null);
        }
    }

    public void onRuleToSelfMsg(RuleNodeToSelfMsg msg) throws Exception {
        checkComponentStateActive(msg.getMsg());
        JnksIotMsg jnksIotMsg = msg.getMsg();
        int ruleNodeCount = jnksIotMsg.getAndIncrementRuleNodeCounter();
        var tenantProfileConfiguration = getTenantProfileConfiguration();
        int maxRuleNodeExecutionsPerMessage = tenantProfileConfiguration.getMaxRuleNodeExecsPerMessage();
        if (maxRuleNodeExecutionsPerMessage == 0 || ruleNodeCount < maxRuleNodeExecutionsPerMessage) {
            apiUsageClient.report(tenantId, jnksIotMsg.getCustomerId(), ApiUsageRecordKey.RE_EXEC_COUNT);
            persistDebugInputIfAllowed(msg.getMsg(), "Self");
            try {
                jnksIotNode.onMsg(defaultCtx, msg.getMsg());
            } catch (Exception e) {
                defaultCtx.tellFailure(msg.getMsg(), e);
            }
        } else {
            jnksIotMsg.getCallback().onFailure(new RuleNodeException("Message is processed by more then " + maxRuleNodeExecutionsPerMessage + " rule nodes!", ruleChainName, ruleNode));
        }
    }

    void onRuleChainToRuleNodeMsg(RuleChainToRuleNodeMsg msg) throws Exception {
        if (!isMyNodePartition()) {
            putToNodePartition(msg.getMsg());
        } else {
            msg.getMsg().getCallback().onProcessingStart(info);
            checkComponentStateActive(msg.getMsg());
            JnksIotMsg jnksIotMsg = msg.getMsg();
            int ruleNodeCount = jnksIotMsg.getAndIncrementRuleNodeCounter();
            var tenantProfileConfiguration = getTenantProfileConfiguration();
            int maxRuleNodeExecutionsPerMessage = tenantProfileConfiguration.getMaxRuleNodeExecsPerMessage();
            if (maxRuleNodeExecutionsPerMessage == 0 || ruleNodeCount < maxRuleNodeExecutionsPerMessage) {
                apiUsageClient.report(tenantId, jnksIotMsg.getCustomerId(), ApiUsageRecordKey.RE_EXEC_COUNT);
                persistDebugInputIfAllowed(msg.getMsg(), msg.getFromRelationType());
                try {
                    jnksIotNode.onMsg(msg.getCtx(), msg.getMsg());
                } catch (Exception e) {
                    msg.getCtx().tellFailure(msg.getMsg(), e);
                }
            } else {
                jnksIotMsg.getCallback().onFailure(new RuleNodeException("Message is processed by more then " + maxRuleNodeExecutionsPerMessage + " rule nodes!", ruleChainName, ruleNode));
            }
        }
    }

    @Override
    public String getComponentName() {
        return getName(ruleNode);
    }

    private String getName(RuleNode ruleNode) {
        return ruleNode != null ? ruleNode.getName() : UNKNOWN_NAME;
    }

    private JnksIotNode initComponent(RuleNode ruleNode) throws Exception {
        JnksIotNode jnksIotNode = null;
        if (ruleNode != null) {
            Class<?> componentClazz = Class.forName(ruleNode.getType());
            jnksIotNode = (JnksIotNode) (componentClazz.getDeclaredConstructor().newInstance());
            jnksIotNode.init(defaultCtx, new JnksIotNodeConfiguration(ruleNode.getConfiguration()));
        }
        return jnksIotNode;
    }

    @Override
    protected RuleNodeException getInactiveException() {
        return new RuleNodeException("Rule Node is not active! Failed to initialize.", ruleChainName, ruleNode);
    }

    private boolean isMyNodePartition() {
        return isMyNodePartition(this.ruleNode);
    }

    private boolean isMyNodePartition(RuleNode ruleNode) {
        boolean result = ruleNode == null || !ruleNode.isSingletonMode()
                || systemContext.getDiscoveryService().isMonolith()
                || defaultCtx.isLocalEntity(ruleNode.getId());
        if (!result) {
            log.trace("[{}][{}] Is not my node partition", tenantId, entityId);
        }
        return result;
    }

    //Message will return after processing. See RuleChainActorMessageProcessor.pushToTarget.
    private void putToNodePartition(JnksIotMsg source) {
        JnksIotMsg jnksIotMsg = JnksIotMsg.newMsg(source, source.getQueueName(), source.getRuleChainId(), entityId);
        TopicPartitionInfo tpi = systemContext.resolve(ServiceType.JNKS_IOT_RULE_ENGINE, jnksIotMsg.getQueueName(), tenantId, ruleNode.getId());
        TransportProtos.ToRuleEngineMsg toQueueMsg = TransportProtos.ToRuleEngineMsg.newBuilder()
                .setTenantIdMSB(tenantId.getId().getMostSignificantBits())
                .setTenantIdLSB(tenantId.getId().getLeastSignificantBits())
                .setJnksIotMsg(JnksIotMsg.toByteString(jnksIotMsg))
                .build();
        systemContext.getClusterService().pushMsgToRuleEngine(tpi, jnksIotMsg.getId(), toQueueMsg, null);
        defaultCtx.ack(source);
    }

    private void persistDebugInputIfAllowed(JnksIotMsg msg, String fromNodeConnectionType) {
        if (DebugModeUtil.isDebugAllAvailable(ruleNode)) {
            systemContext.persistDebugInput(tenantId, entityId, msg, fromNodeConnectionType);
        }
    }

}
