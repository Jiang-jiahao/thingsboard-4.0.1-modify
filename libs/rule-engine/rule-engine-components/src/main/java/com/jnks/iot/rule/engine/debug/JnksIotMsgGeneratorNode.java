package com.jnks.iot.rule.engine.debug;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JnksIotStopWatch;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.ScriptEngine;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.script.ScriptLanguage;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.common.msg.queue.PartitionChangeMsg;

import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.jnks.iot.common.util.DonAsynchron.withCallback;
import static com.jnks.iot.server.common.data.DataConstants.QUEUE_NAME;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "生成器",
        configClazz = JnksIotMsgGeneratorNodeConfiguration.class,
        version = 2,
        hasQueueName = true,
        nodeDescription = "周期性生成消息",
        nodeDetails = "按可配置的周期生成消息。使用 Javascript 函数生成消息。",
        inEnabled = false,
        configDirective = "jnksIotActionNodeGeneratorConfig",
        icon = "repeat"
)

public class JnksIotMsgGeneratorNode implements JnksIotNode {

    private static final Set<EntityType> supportedEntityTypes = EnumSet.of(EntityType.DEVICE, EntityType.ASSET, EntityType.ENTITY_VIEW,
            EntityType.TENANT, EntityType.CUSTOMER, EntityType.USER, EntityType.DASHBOARD, EntityType.RULE_NODE);

    private JnksIotMsgGeneratorNodeConfiguration config;
    private ScriptEngine scriptEngine;
    private long delay;
    private long lastScheduledTs;
    private int currentMsgCount;
    private EntityId originatorId;
    private UUID nextTickId;
    private JnksIotMsg prevMsg;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private String queueName;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotMsgGeneratorNodeConfiguration.class);
        this.delay = TimeUnit.SECONDS.toMillis(config.getPeriodInSeconds());
        this.currentMsgCount = 0;
        this.queueName = ctx.getQueueName();
        if (!supportedEntityTypes.contains(config.getOriginatorType())) {
            throw new JnksIotNodeException("Originator type '" + config.getOriginatorType() + "' is not supported.", true);
        }
        originatorId = getOriginatorId(ctx);
        log.debug("[{}] Initializing generator with config {}", originatorId, configuration);
        updateGeneratorState(ctx);
    }

    @Override
    public void onPartitionChangeMsg(JnksIotContext ctx, PartitionChangeMsg msg) {
        log.debug("[{}] Handling partition change msg: {}", originatorId, msg);
        updateGeneratorState(ctx);
    }

    private void updateGeneratorState(JnksIotContext ctx) {
        log.trace("[{}] Updating generator state, config {}", originatorId, config);
        if (ctx.isLocalEntity(originatorId)) {
            if (initialized.compareAndSet(false, true)) {
                this.scriptEngine = ctx.createScriptEngine(config.getScriptLang(),
                        ScriptLanguage.TBEL.equals(config.getScriptLang()) ? config.getTbelScript() : config.getJsScript(), "prevMsg", "prevMetadata", "prevMsgType");
                scheduleTickMsg(ctx, null);
            }
        } else if (initialized.compareAndSet(true, false)) {
            destroy();
        }
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        log.trace("[{}] onMsg. Expected msg id: {}, msg: {}, config: {}", originatorId, nextTickId, msg, config);
        if (initialized.get() && msg.isTypeOf(JnksIotMsgType.GENERATOR_NODE_SELF_MSG) && msg.getId().equals(nextTickId)) {
            JnksIotStopWatch sw = JnksIotStopWatch.create();
            withCallback(generate(ctx, msg),
                    m -> {
                        log.trace("onMsg onSuccess callback, took {}ms, config {}, msg {}", sw.stopAndGetTotalTimeMillis(), config, msg);
                        if (initialized.get() && (config.getMsgCount() == JnksIotMsgGeneratorNodeConfiguration.UNLIMITED_MSG_COUNT || currentMsgCount < config.getMsgCount())) {
                            ctx.enqueueForTellNext(m, JnksIotNodeConnectionType.SUCCESS);
                            scheduleTickMsg(ctx, msg);
                            currentMsgCount++;
                        }
                    },
                    t -> {
                        log.trace("onMsg onFailure callback, took {}ms, config {}, msg {}", sw.stopAndGetTotalTimeMillis(), config, msg, t);
                        if (initialized.get() && (config.getMsgCount() == JnksIotMsgGeneratorNodeConfiguration.UNLIMITED_MSG_COUNT || currentMsgCount < config.getMsgCount())) {
                            ctx.tellFailure(msg, t);
                            scheduleTickMsg(ctx, msg);
                            currentMsgCount++;
                        }
                    });
        }
    }

    private void scheduleTickMsg(JnksIotContext ctx, JnksIotMsg msg) {
        long curTs = System.currentTimeMillis();
        if (lastScheduledTs == 0L) {
            lastScheduledTs = curTs;
        }
        lastScheduledTs = lastScheduledTs + delay;
        long curDelay = Math.max(0L, (lastScheduledTs - curTs));
        JnksIotMsg tickMsg = ctx.newMsg(queueName, JnksIotMsgType.GENERATOR_NODE_SELF_MSG, ctx.getSelfId(),
                getCustomerIdFromMsg(msg), JnksIotMsgMetaData.EMPTY, JnksIotMsg.EMPTY_STRING);
        nextTickId = tickMsg.getId();
        ctx.tellSelf(tickMsg, curDelay);
        log.trace("[{}] Scheduled tick msg with delay {}, msg: {}, config: {}", originatorId, curDelay, tickMsg, config);
    }

    private ListenableFuture<JnksIotMsg> generate(JnksIotContext ctx, JnksIotMsg msg) {
        log.trace("generate, config {}", config);
        if (prevMsg == null) {
            prevMsg = ctx.newMsg(queueName, JnksIotMsg.EMPTY_STRING, originatorId, msg.getCustomerId(), JnksIotMsgMetaData.EMPTY, JnksIotMsg.EMPTY_JSON_OBJECT);
        }
        if (initialized.get()) {
            return Futures.transformAsync(scriptEngine.executeGenerateAsync(prevMsg), generated -> {
                log.trace("generate process response, generated {}, config {}", generated, config);
                prevMsg = ctx.newMsg(queueName, generated.getType(), originatorId, msg.getCustomerId(), generated.getMetaData(), generated.getData());
                return Futures.immediateFuture(prevMsg);
            }, MoreExecutors.directExecutor()); //usually it runs on js-executor-remote-callback thread pool
        }
        return Futures.immediateFuture(prevMsg);

    }

    private CustomerId getCustomerIdFromMsg(JnksIotMsg msg) {
        return msg != null ? msg.getCustomerId() : null;
    }

    private EntityId getOriginatorId(JnksIotContext ctx) throws JnksIotNodeException {
        if (EntityType.RULE_NODE.equals(config.getOriginatorType())) {
            return ctx.getSelfId();
        }
        if (EntityType.TENANT.equals(config.getOriginatorType())) {
            return ctx.getTenantId();
        }
        if (StringUtils.isBlank(config.getOriginatorId())) {
            throw new JnksIotNodeException("Originator entity must be selected.", true);
        }
        var entityId = EntityIdFactory.getByTypeAndUuid(config.getOriginatorType(), config.getOriginatorId());
        ctx.checkTenantEntity(entityId);
        return entityId;
    }

    @Override
    public void destroy() {
        log.debug("[{}] Stopping generator", originatorId);
        initialized.set(false);
        prevMsg = null;
        nextTickId = null;
        lastScheduledTs = 0;
        if (scriptEngine != null) {
            scriptEngine.destroy();
            scriptEngine = null;
        }
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        boolean hasChanges = false;
        switch (fromVersion) {
            case 0:
                if (oldConfiguration.has(QUEUE_NAME)) {
                    hasChanges = true;
                    ((ObjectNode) oldConfiguration).remove(QUEUE_NAME);
                }
            case 1:
                String originatorType = "originatorType";
                String originatorId = "originatorId";
                boolean hasType = oldConfiguration.hasNonNull(originatorType);
                boolean hasOriginatorId = oldConfiguration.hasNonNull(originatorId) &&
                        StringUtils.isNotBlank(oldConfiguration.get(originatorId).asText());
                boolean hasOriginatorFields = hasType && hasOriginatorId;
                if (!hasOriginatorFields) {
                    hasChanges = true;
                    ((ObjectNode) oldConfiguration).put(originatorType, EntityType.RULE_NODE.name());
                }
                break;
            default:
                break;
        }
        return new JnksIotPair<>(hasChanges, oldConfiguration);
    }
}
