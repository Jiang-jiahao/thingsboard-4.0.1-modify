package com.jnks.iot.server.actors.ruleChain;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.channel.EventLoopGroup;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.Arrays;
import com.jnks.iot.common.util.ExecutorProvider;
import com.jnks.iot.common.util.DebugModeUtil;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.common.util.ListeningExecutor;
import com.jnks.iot.rule.engine.api.MailService;
import com.jnks.iot.rule.engine.api.NotificationCenter;
import com.jnks.iot.rule.engine.api.RuleEngineAlarmService;
import com.jnks.iot.rule.engine.api.RuleEngineApiUsageStateService;
import com.jnks.iot.rule.engine.api.RuleEngineAssetProfileCache;
import com.jnks.iot.rule.engine.api.RuleEngineCalculatedFieldQueueService;
import com.jnks.iot.rule.engine.api.RuleEngineDeviceProfileCache;
import com.jnks.iot.rule.engine.api.DeviceStateManager;
import com.jnks.iot.rule.engine.api.RuleEngineRpcService;
import com.jnks.iot.rule.engine.api.RuleEngineTelemetryService;
import com.jnks.iot.rule.engine.api.ScriptEngine;
import com.jnks.iot.rule.engine.api.SmsService;
import com.jnks.iot.rule.engine.api.TbContext;
import com.jnks.iot.rule.engine.api.TbNodeException;
import com.jnks.iot.rule.engine.api.notification.SlackService;
import com.jnks.iot.rule.engine.api.sms.SmsSenderFactory;
import com.jnks.iot.rule.engine.util.TenantIdLoader;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.TbActorRef;
import com.jnks.iot.server.cluster.TbClusterService;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.HasRuleEngineProfile;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.asset.Asset;
import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.msg.TbMsgType;
import com.jnks.iot.server.common.data.msg.TbNodeConnectionType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.rule.RuleNode;
import com.jnks.iot.server.common.data.rule.RuleNodeState;
import com.jnks.iot.server.common.data.script.ScriptLanguage;
import com.jnks.iot.server.common.msg.TbActorMsg;
import com.jnks.iot.server.common.msg.TbMsg;
import com.jnks.iot.server.common.msg.TbMsgMetaData;
import com.jnks.iot.server.common.msg.TbMsgProcessingStackItem;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.dao.alarm.AlarmCommentService;
import com.jnks.iot.server.dao.asset.AssetProfileService;
import com.jnks.iot.server.dao.asset.AssetService;
import com.jnks.iot.server.dao.attributes.AttributesService;
import com.jnks.iot.server.dao.audit.AuditLogService;
import com.jnks.iot.server.dao.cassandra.CassandraCluster;
import com.jnks.iot.server.dao.cf.CalculatedFieldService;
import com.jnks.iot.server.dao.customer.CustomerService;
import com.jnks.iot.server.dao.dashboard.DashboardService;
import com.jnks.iot.server.dao.device.DeviceCredentialsService;
import com.jnks.iot.server.dao.device.DeviceProfileService;
import com.jnks.iot.server.dao.device.DeviceService;
import com.jnks.iot.server.dao.domain.DomainService;
import com.jnks.iot.server.dao.entity.EntityService;
import com.jnks.iot.server.dao.entityview.EntityViewService;
import com.jnks.iot.server.dao.event.EventService;
import com.jnks.iot.server.dao.mobile.MobileAppBundleService;
import com.jnks.iot.server.dao.mobile.MobileAppService;
import com.jnks.iot.server.dao.nosql.CassandraStatementTask;
import com.jnks.iot.server.dao.nosql.TbResultSetFuture;
import com.jnks.iot.server.dao.notification.NotificationRequestService;
import com.jnks.iot.server.dao.notification.NotificationRuleService;
import com.jnks.iot.server.dao.notification.NotificationTargetService;
import com.jnks.iot.server.dao.notification.NotificationTemplateService;
import com.jnks.iot.server.dao.oauth2.OAuth2ClientService;
import com.jnks.iot.server.dao.ota.OtaPackageService;
import com.jnks.iot.server.dao.queue.QueueService;
import com.jnks.iot.server.dao.queue.QueueStatsService;
import com.jnks.iot.server.dao.relation.RelationService;
import com.jnks.iot.server.dao.resource.ResourceService;
import com.jnks.iot.server.dao.rule.RuleChainService;
import com.jnks.iot.server.dao.tenant.TenantService;
import com.jnks.iot.server.dao.timeseries.TimeseriesService;
import com.jnks.iot.server.dao.user.UserService;
import com.jnks.iot.server.dao.widget.WidgetTypeService;
import com.jnks.iot.server.dao.widget.WidgetsBundleService;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.queue.TbQueueCallback;
import com.jnks.iot.server.queue.common.SimpleTbQueueCallback;
import com.jnks.iot.server.service.script.RuleNodeJsScriptEngine;
import com.jnks.iot.server.service.script.RuleNodeTbelScriptEngine;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.jnks.iot.server.common.data.msg.TbMsgType.ATTRIBUTES_DELETED;
import static com.jnks.iot.server.common.data.msg.TbMsgType.ATTRIBUTES_UPDATED;
import static com.jnks.iot.server.common.data.msg.TbMsgType.ENTITY_CREATED;

/**
 * Created by ashvayka on 19.03.18.
 */
@Slf4j
public class DefaultTbContext implements TbContext {

    private final ActorSystemContext mainCtx;
    private final String ruleChainName;
    private final RuleNodeCtx nodeCtx;

    public DefaultTbContext(ActorSystemContext mainCtx, String ruleChainName, RuleNodeCtx nodeCtx) {
        this.mainCtx = mainCtx;
        this.ruleChainName = ruleChainName;
        this.nodeCtx = nodeCtx;
    }

    @Override
    public void tellSuccess(TbMsg msg) {
        tellNext(msg, Collections.singleton(TbNodeConnectionType.SUCCESS));
    }

    @Override
    public void tellNext(TbMsg msg, String relationType) {
        tellNext(msg, Collections.singleton(relationType));
    }

    @Override
    public void tellNext(TbMsg msg, Set<String> relationTypes) {
        RuleNode ruleNode = nodeCtx.getSelf();
        persistDebugOutput(msg, relationTypes);
        msg.getCallback().onProcessingEnd(ruleNode.getId());
        nodeCtx.getChainActor().tell(new RuleNodeToRuleChainTellNextMsg(ruleNode.getRuleChainId(), ruleNode.getId(), relationTypes, msg, null));
    }

    @Override
    public void tellSelf(TbMsg msg, long delayMs) {
        //TODO: add persistence layer
        scheduleMsgWithDelay(new RuleNodeToSelfMsg(this, msg), delayMs, nodeCtx.getSelfActor());
    }

    @Override
    public void input(TbMsg msg, RuleChainId ruleChainId) {
        if (!msg.isValid()) {
            return;
        }
        TbMsg tbMsg = msg.copy()
                .ruleChainId(ruleChainId)
                .resetRuleNodeId()
                .build();
        tbMsg.pushToStack(nodeCtx.getSelf().getRuleChainId(), nodeCtx.getSelf().getId());
        TopicPartitionInfo tpi = resolvePartition(msg);
        doEnqueue(tpi, tbMsg, new SimpleTbQueueCallback(md -> ack(msg), t -> tellFailure(msg, t)));
    }

    @Override
    public void output(TbMsg msg, String relationType) {
        TbMsgProcessingStackItem item = msg.popFormStack();
        if (item == null) {
            ack(msg);
        } else {
            persistDebugOutput(msg, relationType);
            nodeCtx.getChainActor().tell(new RuleChainOutputMsg(item.getRuleChainId(), item.getRuleNodeId(), relationType, msg));
        }
    }

    @Override
    public void enqueue(TbMsg tbMsg, Runnable onSuccess, Consumer<Throwable> onFailure) {
        enqueue(tbMsg, tbMsg.getQueueName(), onSuccess, onFailure);
    }

    @Override
    public void enqueue(TbMsg tbMsg, String queueName, Runnable onSuccess, Consumer<Throwable> onFailure) {
        TopicPartitionInfo tpi = resolvePartition(tbMsg, queueName);
        enqueue(tpi, tbMsg, onFailure, onSuccess);
    }

    private void enqueue(TopicPartitionInfo tpi, TbMsg tbMsg, Consumer<Throwable> onFailure, Runnable onSuccess) {
        if (!tbMsg.isValid()) {
            log.trace("[{}] Skip invalid message: {}", getTenantId(), tbMsg);
            if (onFailure != null) {
                onFailure.accept(new IllegalArgumentException("Source message is no longer valid!"));
            }
            return;
        }
        doEnqueue(tpi, tbMsg, new SimpleTbQueueCallback(
                metadata -> {
                    persistDebugOutput(tbMsg, TbNodeConnectionType.TO_ROOT_RULE_CHAIN);
                    if (onSuccess != null) {
                        onSuccess.run();
                    }
                },
                t -> {
                    if (onFailure != null) {
                        onFailure.accept(t);
                    } else {
                        log.debug("[{}] Failed to put item into queue!", nodeCtx.getTenantId().getId(), t);
                    }
                }));
    }

    private void doEnqueue(TopicPartitionInfo tpi, TbMsg tbMsg, TbQueueCallback callback) {
        TransportProtos.ToRuleEngineMsg msg = TransportProtos.ToRuleEngineMsg.newBuilder()
                .setTenantIdMSB(getTenantId().getId().getMostSignificantBits())
                .setTenantIdLSB(getTenantId().getId().getLeastSignificantBits())
                .setTbMsg(TbMsg.toByteString(tbMsg)).build();
        mainCtx.getClusterService().pushMsgToRuleEngine(tpi, tbMsg.getId(), msg, callback);
    }

    @Override
    public void enqueueForTellFailure(TbMsg tbMsg, String failureMessage) {
        TopicPartitionInfo tpi = resolvePartition(tbMsg);
        enqueueForTellNext(tpi, tbMsg, Collections.singleton(TbNodeConnectionType.FAILURE), failureMessage, null, null);
    }

    @Override
    public void enqueueForTellFailure(TbMsg tbMsg, Throwable th) {
        TopicPartitionInfo tpi = resolvePartition(tbMsg);
        enqueueForTellNext(tpi, tbMsg, Collections.singleton(TbNodeConnectionType.FAILURE), getFailureMessage(th), null, null);
    }

    @Override
    public void enqueueForTellNext(TbMsg tbMsg, String relationType) {
        TopicPartitionInfo tpi = resolvePartition(tbMsg);
        enqueueForTellNext(tpi, tbMsg, Collections.singleton(relationType), null, null, null);
    }

    @Override
    public void enqueueForTellNext(TbMsg tbMsg, Set<String> relationTypes) {
        TopicPartitionInfo tpi = resolvePartition(tbMsg);
        enqueueForTellNext(tpi, tbMsg, relationTypes, null, null, null);
    }

    @Override
    public void enqueueForTellNext(TbMsg tbMsg, String relationType, Runnable onSuccess, Consumer<Throwable> onFailure) {
        TopicPartitionInfo tpi = resolvePartition(tbMsg);
        enqueueForTellNext(tpi, tbMsg, Collections.singleton(relationType), null, onSuccess, onFailure);
    }

    @Override
    public void enqueueForTellNext(TbMsg tbMsg, Set<String> relationTypes, Runnable onSuccess, Consumer<Throwable> onFailure) {
        TopicPartitionInfo tpi = resolvePartition(tbMsg);
        enqueueForTellNext(tpi, tbMsg, relationTypes, null, onSuccess, onFailure);
    }

    @Override
    public void enqueueForTellNext(TbMsg tbMsg, String queueName, String relationType, Runnable onSuccess, Consumer<Throwable> onFailure) {
        TopicPartitionInfo tpi = resolvePartition(tbMsg, queueName);
        enqueueForTellNext(tpi, queueName, tbMsg, Collections.singleton(relationType), null, onSuccess, onFailure);
    }

    @Override
    public void enqueueForTellNext(TbMsg tbMsg, String queueName, Set<String> relationTypes, Runnable onSuccess, Consumer<Throwable> onFailure) {
        TopicPartitionInfo tpi = resolvePartition(tbMsg, queueName);
        enqueueForTellNext(tpi, queueName, tbMsg, relationTypes, null, onSuccess, onFailure);
    }

    private TopicPartitionInfo resolvePartition(TbMsg tbMsg, String queueName) {
        return mainCtx.resolve(ServiceType.TB_RULE_ENGINE, queueName, getTenantId(), tbMsg.getOriginator());
    }

    private TopicPartitionInfo resolvePartition(TbMsg tbMsg) {
        return resolvePartition(tbMsg, tbMsg.getQueueName());
    }

    private void enqueueForTellNext(TopicPartitionInfo tpi, TbMsg source, Set<String> relationTypes, String failureMessage, Runnable onSuccess, Consumer<Throwable> onFailure) {
        enqueueForTellNext(tpi, source.getQueueName(), source, relationTypes, failureMessage, onSuccess, onFailure);
    }

    private void enqueueForTellNext(TopicPartitionInfo tpi, String queueName, TbMsg source, Set<String> relationTypes, String failureMessage, Runnable onSuccess, Consumer<Throwable> onFailure) {
        if (!source.isValid()) {
            log.trace("[{}] Skip invalid message: {}", getTenantId(), source);
            if (onFailure != null) {
                onFailure.accept(new IllegalArgumentException("Source message is no longer valid!"));
            }
            return;
        }
        RuleNode ruleNode = nodeCtx.getSelf();
        RuleChainId ruleChainId = ruleNode.getRuleChainId();
        RuleNodeId ruleNodeId = ruleNode.getId();
        TbMsg tbMsg = TbMsg.newMsg(source, queueName, ruleChainId, ruleNodeId);
        TransportProtos.ToRuleEngineMsg.Builder msg = TransportProtos.ToRuleEngineMsg.newBuilder()
                .setTenantIdMSB(getTenantId().getId().getMostSignificantBits())
                .setTenantIdLSB(getTenantId().getId().getLeastSignificantBits())
                .setTbMsg(TbMsg.toByteString(tbMsg))
                .addAllRelationTypes(relationTypes);
        if (failureMessage != null) {
            msg.setFailureMessage(failureMessage);
        }
        mainCtx.getClusterService().pushMsgToRuleEngine(tpi, tbMsg.getId(), msg.build(), new SimpleTbQueueCallback(
                metadata -> {
                    persistDebugOutput(tbMsg, relationTypes, null, failureMessage);
                    if (onSuccess != null) {
                        onSuccess.run();
                    }
                },
                t -> {
                    if (onFailure != null) {
                        onFailure.accept(t);
                    } else {
                        log.debug("[{}] Failed to put item into queue!", nodeCtx.getTenantId().getId(), t);
                    }
                }));
    }

    @Override
    public void ack(TbMsg tbMsg) {
        RuleNode ruleNode = nodeCtx.getSelf();
        persistDebugOutput(tbMsg, TbNodeConnectionType.ACK);
        tbMsg.getCallback().onProcessingEnd(ruleNode.getId());
        tbMsg.getCallback().onSuccess();
    }

    @Override
    public boolean isLocalEntity(EntityId entityId) {
        return mainCtx.resolve(ServiceType.TB_RULE_ENGINE, getQueueName(), getTenantId(), entityId).isMyPartition();
    }

    private void scheduleMsgWithDelay(TbActorMsg msg, long delayInMs, TbActorRef target) {
        mainCtx.scheduleMsgWithDelay(target, msg, delayInMs);
    }

    @Override
    public void tellFailure(TbMsg msg, Throwable th) {
        RuleNode ruleNode = nodeCtx.getSelf();
        persistDebugOutput(msg, Set.of(TbNodeConnectionType.FAILURE), th, null);
        String failureMessage = getFailureMessage(th);
        nodeCtx.getChainActor().tell(new RuleNodeToRuleChainTellNextMsg(ruleNode.getRuleChainId(),
                ruleNode.getId(), Collections.singleton(TbNodeConnectionType.FAILURE),
                msg, failureMessage));
    }

    public void updateSelf(RuleNode self) {
        nodeCtx.setSelf(self);
    }

    @Override
    public TbMsg newMsg(String queueName, String type, EntityId originator, CustomerId customerId, TbMsgMetaData metaData, String data) {
        return TbMsg.newMsg()
                .queueName(queueName)
                .type(type)
                .originator(originator)
                .customerId(customerId)
                .copyMetaData(metaData)
                .data(data)
                .ruleChainId(nodeCtx.getSelf().getRuleChainId())
                .ruleNodeId(nodeCtx.getSelf().getId())
                .build();
    }

    @Override
    public TbMsg transformMsg(TbMsg origMsg, String type, EntityId originator, TbMsgMetaData metaData, String data) {
        return origMsg.transform()
                .type(type)
                .originator(originator)
                .metaData(metaData)
                .data(data)
                .build();
    }

    @Override
    public TbMsg newMsg(String queueName, TbMsgType type, EntityId originator, TbMsgMetaData metaData, String data) {
        return newMsg(queueName, type, originator, null, metaData, data);
    }

    @Override
    public TbMsg newMsg(String queueName, TbMsgType type, EntityId originator, CustomerId customerId, TbMsgMetaData metaData, String data) {
        return TbMsg.newMsg()
                .queueName(queueName)
                .type(type)
                .originator(originator)
                .customerId(customerId)
                .copyMetaData(metaData)
                .data(data)
                .ruleChainId(nodeCtx.getSelf().getRuleChainId())
                .ruleNodeId(nodeCtx.getSelf().getId())
                .build();
    }

    @Override
    public TbMsg transformMsg(TbMsg origMsg, TbMsgType type, EntityId originator, TbMsgMetaData metaData, String data) {
        return origMsg.transform()
                .type(type)
                .originator(originator)
                .metaData(metaData)
                .data(data)
                .build();
    }

    @Override
    public TbMsg transformMsg(TbMsg origMsg, TbMsgMetaData metaData, String data) {
        return origMsg.transform()
                .metaData(metaData)
                .data(data)
                .build();
    }

    @Override
    public TbMsg transformMsgOriginator(TbMsg origMsg, EntityId originator) {
        return origMsg.transform()
                .originator(originator)
                .build();
    }

    @Override
    public TbMsg customerCreatedMsg(Customer customer, RuleNodeId ruleNodeId) {
        return entityActionMsg(customer, customer.getId(), ruleNodeId, ENTITY_CREATED);
    }

    @Override
    public TbMsg deviceCreatedMsg(Device device, RuleNodeId ruleNodeId) {
        DeviceProfile deviceProfile = null;
        if (device.getDeviceProfileId() != null) {
            deviceProfile = mainCtx.getDeviceProfileCache().find(device.getDeviceProfileId());
        }
        return entityActionMsg(device, device.getId(), ruleNodeId, ENTITY_CREATED, deviceProfile);
    }

    @Override
    public TbMsg assetCreatedMsg(Asset asset, RuleNodeId ruleNodeId) {
        AssetProfile assetProfile = null;
        if (asset.getAssetProfileId() != null) {
            assetProfile = mainCtx.getAssetProfileCache().find(asset.getAssetProfileId());
        }
        return entityActionMsg(asset, asset.getId(), ruleNodeId, ENTITY_CREATED, assetProfile);
    }

    @Override
    public TbMsg alarmActionMsg(Alarm alarm, RuleNodeId ruleNodeId, String action) {
        EntityId originator = alarm.getOriginator();
        HasRuleEngineProfile profile = getRuleEngineProfile(originator);
        return entityActionMsg(alarm, originator, ruleNodeId, action, profile);
    }

    @Override
    public TbMsg alarmActionMsg(Alarm alarm, RuleNodeId ruleNodeId, TbMsgType actionMsgType) {
        EntityId originator = alarm.getOriginator();
        HasRuleEngineProfile profile = getRuleEngineProfile(originator);
        return entityActionMsg(alarm, originator, ruleNodeId, actionMsgType, profile);
    }

    private HasRuleEngineProfile getRuleEngineProfile(EntityId originator) {
        HasRuleEngineProfile profile = null;
        if (EntityType.DEVICE.equals(originator.getEntityType())) {
            DeviceId deviceId = new DeviceId(originator.getId());
            profile = mainCtx.getDeviceProfileCache().get(getTenantId(), deviceId);
        } else if (EntityType.ASSET.equals(originator.getEntityType())) {
            AssetId assetId = new AssetId(originator.getId());
            profile = mainCtx.getAssetProfileCache().get(getTenantId(), assetId);
        }
        return profile;
    }

    @Override
    public TbMsg attributesUpdatedActionMsg(EntityId originator, RuleNodeId ruleNodeId, String scope, List<AttributeKvEntry> attributes) {
        ObjectNode entityNode = JacksonUtil.newObjectNode();
        if (attributes != null) {
            attributes.forEach(attributeKvEntry -> JacksonUtil.addKvEntry(entityNode, attributeKvEntry));
        }
        return attributesActionMsg(originator, ruleNodeId, scope, ATTRIBUTES_UPDATED, JacksonUtil.toString(entityNode));
    }

    @Override
    public TbMsg attributesDeletedActionMsg(EntityId originator, RuleNodeId ruleNodeId, String scope, List<String> keys) {
        ObjectNode entityNode = JacksonUtil.newObjectNode();
        ArrayNode attrsArrayNode = entityNode.putArray("attributes");
        if (keys != null) {
            keys.forEach(attrsArrayNode::add);
        }
        return attributesActionMsg(originator, ruleNodeId, scope, ATTRIBUTES_DELETED, JacksonUtil.toString(entityNode));
    }

    private TbMsg attributesActionMsg(EntityId originator, RuleNodeId ruleNodeId, String scope, TbMsgType actionMsgType, String msgData) {
        TbMsgMetaData tbMsgMetaData = getActionMetaData(ruleNodeId);
        tbMsgMetaData.putValue("scope", scope);
        HasRuleEngineProfile profile = getRuleEngineProfile(originator);
        return entityActionMsg(originator, tbMsgMetaData, msgData, actionMsgType, profile);
    }

    public <E, I extends EntityId> TbMsg entityActionMsg(E entity, I id, RuleNodeId ruleNodeId, TbMsgType actionMsgType) {
        return entityActionMsg(entity, id, ruleNodeId, actionMsgType, null);
    }

    @Deprecated(since = "3.6.0", forRemoval = true)
    public <E, I extends EntityId, K extends HasRuleEngineProfile> TbMsg entityActionMsg(E entity, I id, RuleNodeId ruleNodeId, String action, K profile) {
        try {
            return entityActionMsg(id, getActionMetaData(ruleNodeId), JacksonUtil.toString(JacksonUtil.valueToTree(entity)), action, profile);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Failed to process " + id.getEntityType().name().toLowerCase() + " " + action + " msg: " + e);
        }
    }

    @Deprecated(since = "3.6.0", forRemoval = true)
    private <I extends EntityId, K extends HasRuleEngineProfile> TbMsg entityActionMsg(I id, TbMsgMetaData msgMetaData, String msgData, String action, K profile) {
        String defaultQueueName = null;
        RuleChainId defaultRuleChainId = null;
        if (profile != null) {
            defaultQueueName = profile.getDefaultQueueName();
            defaultRuleChainId = profile.getDefaultRuleChainId();
        }
        return TbMsg.newMsg()
                .queueName(defaultQueueName)
                .type(action)
                .originator(id)
                .copyMetaData(msgMetaData)
                .data(msgData)
                .ruleChainId(defaultRuleChainId)
                .build();
    }

    public <E, I extends EntityId, K extends HasRuleEngineProfile> TbMsg entityActionMsg(E entity, I id, RuleNodeId ruleNodeId, TbMsgType actionMsgType, K profile) {
        try {
            return entityActionMsg(id, getActionMetaData(ruleNodeId), JacksonUtil.toString(JacksonUtil.valueToTree(entity)), actionMsgType, profile);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Failed to process " + id.getEntityType().name().toLowerCase() + " " + actionMsgType.name() + " msg: " + e);
        }
    }

    private <I extends EntityId, K extends HasRuleEngineProfile> TbMsg entityActionMsg(I id, TbMsgMetaData msgMetaData, String msgData, TbMsgType actionMsgType, K profile) {
        String defaultQueueName = null;
        RuleChainId defaultRuleChainId = null;
        if (profile != null) {
            defaultQueueName = profile.getDefaultQueueName();
            defaultRuleChainId = profile.getDefaultRuleChainId();
        }
        return TbMsg.newMsg()
                .queueName(defaultQueueName)
                .type(actionMsgType)
                .originator(id)
                .copyMetaData(msgMetaData)
                .data(msgData)
                .ruleChainId(defaultRuleChainId)
                .build();
    }

    @Override
    public RuleNodeId getSelfId() {
        return nodeCtx.getSelf().getId();
    }

    @Override
    public RuleNode getSelf() {
        return nodeCtx.getSelf();
    }

    @Override
    public String getRuleChainName() {
        return ruleChainName;
    }

    @Override
    public String getQueueName() {
        return getSelf().getQueueName();
    }

    @Override
    public TenantId getTenantId() {
        return nodeCtx.getTenantId();
    }

    @Override
    public ListeningExecutor getMailExecutor() {
        return mainCtx.getMailExecutor();
    }

    @Override
    public ListeningExecutor getSmsExecutor() {
        return mainCtx.getSmsExecutor();
    }

    @Override
    public ListeningExecutor getDbCallbackExecutor() {
        return mainCtx.getDbCallbackExecutor();
    }

    @Override
    public ListeningExecutor getExternalCallExecutor() {
        return mainCtx.getExternalCallExecutorService();
    }

    @Override
    public ListeningExecutor getNotificationExecutor() {
        return mainCtx.getNotificationExecutor();
    }

    @Override
    public ExecutorProvider getPubSubRuleNodeExecutorProvider() {
        return mainCtx.getPubSubRuleNodeExecutorProvider();
    }

    @Override
    @Deprecated
    public ScriptEngine createJsScriptEngine(String script, String... argNames) {
        return new RuleNodeJsScriptEngine(getTenantId(), mainCtx.getJsInvokeService(), script, argNames);
    }

    private ScriptEngine createTbelScriptEngine(String script, String... argNames) {
        if (mainCtx.getTbelInvokeService() == null) {
            throw new RuntimeException("TBEL execution is disabled!");
        }
        return new RuleNodeTbelScriptEngine(getTenantId(), mainCtx.getTbelInvokeService(), script, argNames);
    }

    @Override
    public ScriptEngine createScriptEngine(ScriptLanguage scriptLang, String script, String... argNames) {
        if (scriptLang == null) {
            scriptLang = ScriptLanguage.JS;
        }
        if (StringUtils.isBlank(script)) {
            throw new RuntimeException(scriptLang.name() + " script is blank!");
        }
        switch (scriptLang) {
            case JS:
                return createJsScriptEngine(script, argNames);
            case TBEL:
                if (Arrays.isNullOrEmpty(argNames)) {
                    return createTbelScriptEngine(script, "msg", "metadata", "msgType");
                } else {
                    return createTbelScriptEngine(script, argNames);
                }
            default:
                throw new RuntimeException("Unsupported script language: " + scriptLang.name());
        }
    }

    @Override
    public String getServiceId() {
        return mainCtx.getServiceInfoProvider().getServiceId();
    }

    @Override
    public AttributesService getAttributesService() {
        return mainCtx.getAttributesService();
    }

    @Override
    public CustomerService getCustomerService() {
        return mainCtx.getCustomerService();
    }

    @Override
    public TenantService getTenantService() {
        return mainCtx.getTenantService();
    }

    @Override
    public UserService getUserService() {
        return mainCtx.getUserService();
    }

    @Override
    public AssetService getAssetService() {
        return mainCtx.getAssetService();
    }

    @Override
    public DeviceService getDeviceService() {
        return mainCtx.getDeviceService();
    }

    @Override
    public DeviceProfileService getDeviceProfileService() {
        return mainCtx.getDeviceProfileService();
    }

    @Override
    public AssetProfileService getAssetProfileService() {
        return mainCtx.getAssetProfileService();
    }

    @Override
    public DeviceCredentialsService getDeviceCredentialsService() {
        return mainCtx.getDeviceCredentialsService();
    }

    @Override
    public DeviceStateManager getDeviceStateManager() {
        return mainCtx.getDeviceStateManager();
    }

    @Override
    public String getDeviceStateNodeRateLimitConfig() {
        return mainCtx.getDeviceStateNodeRateLimitConfig();
    }

    @Override
    public TbClusterService getClusterService() {
        return mainCtx.getClusterService();
    }

    @Override
    public DashboardService getDashboardService() {
        return mainCtx.getDashboardService();
    }

    @Override
    public RuleEngineAlarmService getAlarmService() {
        return mainCtx.getAlarmService();
    }

    @Override
    public AlarmCommentService getAlarmCommentService() {
        return mainCtx.getAlarmCommentService();
    }

    @Override
    public RuleChainService getRuleChainService() {
        return mainCtx.getRuleChainService();
    }

    @Override
    public TimeseriesService getTimeseriesService() {
        return mainCtx.getTsService();
    }

    @Override
    public RuleEngineTelemetryService getTelemetryService() {
        return mainCtx.getTsSubService();
    }

    @Override
    public RelationService getRelationService() {
        return mainCtx.getRelationService();
    }

    @Override
    public EntityViewService getEntityViewService() {
        return mainCtx.getEntityViewService();
    }

    @Override
    public ResourceService getResourceService() {
        return mainCtx.getResourceService();
    }

    @Override
    public OtaPackageService getOtaPackageService() {
        return mainCtx.getOtaPackageService();
    }

    @Override
    public RuleEngineDeviceProfileCache getDeviceProfileCache() {
        return mainCtx.getDeviceProfileCache();
    }

    @Override
    public RuleEngineAssetProfileCache getAssetProfileCache() {
        return mainCtx.getAssetProfileCache();
    }

    @Override
    public QueueService getQueueService() {
        return mainCtx.getQueueService();
    }

    @Override
    public QueueStatsService getQueueStatsService() {
        return mainCtx.getQueueStatsService();
    }

    @Override
    public EventLoopGroup getSharedEventLoop() {
        return mainCtx.getSharedEventLoopGroupService().getSharedEventLoopGroup();
    }

    @Override
    public MailService getMailService(boolean isSystem) {
        if (!isSystem || mainCtx.isAllowSystemMailService()) {
            return mainCtx.getMailService();
        } else {
            throw new RuntimeException("Access to System Mail Service is forbidden!");
        }
    }

    @Override
    public SmsService getSmsService() {
        if (mainCtx.isAllowSystemSmsService()) {
            return mainCtx.getSmsService();
        } else {
            throw new RuntimeException("Access to System SMS Service is forbidden!");
        }
    }

    @Override
    public SmsSenderFactory getSmsSenderFactory() {
        return mainCtx.getSmsSenderFactory();
    }

    @Override
    public NotificationCenter getNotificationCenter() {
        return mainCtx.getNotificationCenter();
    }

    @Override
    public NotificationTargetService getNotificationTargetService() {
        return mainCtx.getNotificationTargetService();
    }

    @Override
    public NotificationTemplateService getNotificationTemplateService() {
        return mainCtx.getNotificationTemplateService();
    }

    @Override
    public NotificationRequestService getNotificationRequestService() {
        return mainCtx.getNotificationRequestService();
    }

    @Override
    public NotificationRuleService getNotificationRuleService() {
        return mainCtx.getNotificationRuleService();
    }

    @Override
    public OAuth2ClientService getOAuth2ClientService() {
        return mainCtx.getOAuth2ClientService();
    }

    @Override
    public DomainService getDomainService() {
        return mainCtx.getDomainService();
    }

    @Override
    public MobileAppService getMobileAppService() {
        return mainCtx.getMobileAppService();
    }

    @Override
    public MobileAppBundleService getMobileAppBundleService() {
        return mainCtx.getMobileAppBundleService();
    }

    @Override
    public SlackService getSlackService() {
        return mainCtx.getSlackService();
    }

    @Override
    public CalculatedFieldService getCalculatedFieldService() {
        return mainCtx.getCalculatedFieldService();
    }

    @Override
    public RuleEngineCalculatedFieldQueueService getCalculatedFieldQueueService() {
        return mainCtx.getCalculatedFieldQueueService();
    }

    @Override
    public boolean isExternalNodeForceAck() {
        return mainCtx.isExternalNodeForceAck();
    }

    @Override
    public RuleEngineRpcService getRpcService() {
        return mainCtx.getTbRuleEngineDeviceRpcService();
    }

    @Override
    public CassandraCluster getCassandraCluster() {
        return mainCtx.getCassandraCluster();
    }

    @Override
    public TbResultSetFuture submitCassandraReadTask(CassandraStatementTask task) {
        return mainCtx.getCassandraBufferedRateReadExecutor().submit(task);
    }

    @Override
    public TbResultSetFuture submitCassandraWriteTask(CassandraStatementTask task) {
        return mainCtx.getCassandraBufferedRateWriteExecutor().submit(task);
    }

    @Override
    public PageData<RuleNodeState> findRuleNodeStates(PageLink pageLink) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Fetch Rule Node States.", getTenantId(), getSelfId());
        }
        return mainCtx.getRuleNodeStateService().findByRuleNodeId(getTenantId(), getSelfId(), pageLink);
    }

    @Override
    public RuleNodeState findRuleNodeStateForEntity(EntityId entityId) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}][{}] Fetch Rule Node State for entity.", getTenantId(), getSelfId(), entityId);
        }
        return mainCtx.getRuleNodeStateService().findByRuleNodeIdAndEntityId(getTenantId(), getSelfId(), entityId);
    }

    @Override
    public RuleNodeState saveRuleNodeState(RuleNodeState state) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}][{}] Persist Rule Node State for entity: {}", getTenantId(), getSelfId(), state.getEntityId(), state.getStateData());
        }
        state.setRuleNodeId(getSelfId());
        return mainCtx.getRuleNodeStateService().save(getTenantId(), state);
    }

    @Override
    public void clearRuleNodeStates() {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Going to clear rule node states", getTenantId(), getSelfId());
        }
        mainCtx.getRuleNodeStateService().removeByRuleNodeId(getTenantId(), getSelfId());
    }

    @Override
    public void removeRuleNodeStateForEntity(EntityId entityId) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}][{}] Remove Rule Node State for entity.", getTenantId(), getSelfId(), entityId);
        }
        mainCtx.getRuleNodeStateService().removeByRuleNodeIdAndEntityId(getTenantId(), getSelfId(), entityId);
    }

    @Override
    public void addTenantProfileListener(Consumer<TenantProfile> listener) {
        mainCtx.getTenantProfileCache().addListener(getTenantId(), getSelfId(), listener);
    }

    @Override
    public void addDeviceProfileListeners(Consumer<DeviceProfile> profileListener, BiConsumer<DeviceId, DeviceProfile> deviceListener) {
        mainCtx.getDeviceProfileCache().addListener(getTenantId(), getSelfId(), profileListener, deviceListener);
    }

    @Override
    public void addAssetProfileListeners(Consumer<AssetProfile> profileListener, BiConsumer<AssetId, AssetProfile> assetListener) {
        mainCtx.getAssetProfileCache().addListener(getTenantId(), getSelfId(), profileListener, assetListener);
    }

    @Override
    public void removeListeners() {
        mainCtx.getDeviceProfileCache().removeListener(getTenantId(), getSelfId());
        mainCtx.getAssetProfileCache().removeListener(getTenantId(), getSelfId());
        mainCtx.getTenantProfileCache().removeListener(getTenantId(), getSelfId());
    }

    @Override
    public TenantProfile getTenantProfile() {
        return mainCtx.getTenantProfileCache().get(getTenantId());
    }

    @Override
    public WidgetsBundleService getWidgetBundleService() {
        return mainCtx.getWidgetsBundleService();
    }

    @Override
    public WidgetTypeService getWidgetTypeService() {
        return mainCtx.getWidgetTypeService();
    }

    @Override
    public RuleEngineApiUsageStateService getRuleEngineApiUsageStateService() {
        return mainCtx.getApiUsageStateService();
    }

    @Override
    public EntityService getEntityService() {
        return mainCtx.getEntityService();
    }

    @Override
    public EventService getEventService() {
        return mainCtx.getEventService();
    }

    @Override
    public AuditLogService getAuditLogService() {
        return mainCtx.getAuditLogService();
    }

    private TbMsgMetaData getActionMetaData(RuleNodeId ruleNodeId) {
        TbMsgMetaData metaData = new TbMsgMetaData();
        metaData.putValue("ruleNodeId", ruleNodeId.toString());
        return metaData;
    }


    @Override
    public void schedule(Runnable runnable, long delay, TimeUnit timeUnit) {
        mainCtx.getScheduler().schedule(runnable, delay, timeUnit);
    }

    @Override
    public void checkTenantEntity(EntityId entityId) throws TbNodeException {
        if (!this.getTenantId().equals(TenantIdLoader.findTenantId(this, entityId))) {
            throw new TbNodeException("Entity with id: '" + entityId + "' specified in the configuration doesn't belong to the current tenant.", true);
        }
    }

    private static String getFailureMessage(Throwable th) {
        String failureMessage;
        if (th != null) {
            if (!StringUtils.isEmpty(th.getMessage())) {
                failureMessage = th.getMessage();
            } else {
                failureMessage = th.getClass().getSimpleName();
            }
        } else {
            failureMessage = null;
        }
        return failureMessage;
    }

    private void persistDebugOutput(TbMsg msg, String relationType) {
        persistDebugOutput(msg, Set.of(relationType));
    }

    private void persistDebugOutput(TbMsg msg, Set<String> relationTypes) {
        persistDebugOutput(msg, relationTypes, null, null);
    }

    private void persistDebugOutput(TbMsg msg, Set<String> relationTypes, Throwable error, String failureMessage) {
        RuleNode ruleNode = nodeCtx.getSelf();
        if (DebugModeUtil.isDebugAllAvailable(ruleNode)) {
            relationTypes.forEach(relationType -> mainCtx.persistDebugOutput(getTenantId(), ruleNode.getId(), msg, relationType, error, failureMessage));
        } else if (DebugModeUtil.isDebugFailuresAvailable(ruleNode, relationTypes)) {
            mainCtx.persistDebugOutput(getTenantId(), ruleNode.getId(), msg, TbNodeConnectionType.FAILURE, error, failureMessage);
        }
    }

}
