package com.jnks.iot.rule.engine.api;

import io.netty.channel.EventLoopGroup;
import com.jnks.iot.common.util.ExecutorProvider;
import com.jnks.iot.common.util.ListeningExecutor;
import com.jnks.iot.rule.engine.api.notification.SlackService;
import com.jnks.iot.rule.engine.api.sms.SmsSenderFactory;
import com.jnks.iot.server.cluster.JnksIotClusterService;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
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
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.rule.RuleNode;
import com.jnks.iot.server.common.data.rule.RuleNodeState;
import com.jnks.iot.server.common.data.script.ScriptLanguage;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
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
import com.jnks.iot.server.dao.nosql.JnksIotResultSetFuture;
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

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Created by ashvayka on 13.01.18.
 */
public interface JnksIotContext {

    /*
     *
     *  METHODS TO CONTROL THE MESSAGE FLOW
     *
     */

    /**
     * Indicates that message was successfully processed by the rule node.
     * Sends message to all Rule Nodes in the Rule Chain
     * that are connected to the current Rule Node using "Success" relationType.
     *
     * @param msg
     */
    void tellSuccess(JnksIotMsg msg);

    /**
     * Sends message to all Rule Nodes in the Rule Chain
     * that are connected to the current Rule Node using specified relationType.
     *
     * @param msg
     * @param relationType
     */
    void tellNext(JnksIotMsg msg, String relationType);

    /**
     * Sends message to all Rule Nodes in the Rule Chain
     * that are connected to the current Rule Node using one of specified relationTypes.
     *
     * @param msg
     * @param relationTypes
     */
    void tellNext(JnksIotMsg msg, Set<String> relationTypes);

    /**
     * Sends message to the current Rule Node with specified delay in milliseconds.
     * Note: this message is not queued and may be lost in case of a server restart.
     *
     * @param msg
     */
    void tellSelf(JnksIotMsg msg, long delayMs);

    /**
     * Notifies Rule Engine about failure to process current message.
     *
     * @param msg - message
     * @param th  - exception
     */
    void tellFailure(JnksIotMsg msg, Throwable th);

    /**
     * Puts new message to queue from JnksIotMsg for processing by the Root Rule Chain
     *
     * @param msg - message
     */
    void enqueue(JnksIotMsg msg, Runnable onSuccess, Consumer<Throwable> onFailure);

    /**
     * Puts new message to custom queue for processing
     *
     * @param msg - message
     */
    void enqueue(JnksIotMsg msg, String queueName, Runnable onSuccess, Consumer<Throwable> onFailure);

    /**
     * Sends message to the nested rule chain.
     * Fails processing of the message if the nested rule chain is not found.
     *
     * @param msg - the message
     * @param ruleChainId - the id of a nested rule chain
     */
    void input(JnksIotMsg msg, RuleChainId ruleChainId);

    /**
     * Sends message to the caller rule chain.
     * Acknowledge the message if no caller rule chain is present in processing stack
     *
     * @param msg - the message
     * @param relationType - the relation type that will be used to route messages in the caller rule chain
     */
    void output(JnksIotMsg msg, String relationType);

    void enqueueForTellFailure(JnksIotMsg msg, String failureMessage);

    void enqueueForTellFailure(JnksIotMsg jnksIotMsg, Throwable t);

    void enqueueForTellNext(JnksIotMsg msg, String relationType);

    void enqueueForTellNext(JnksIotMsg msg, Set<String> relationTypes);

    void enqueueForTellNext(JnksIotMsg msg, String relationType, Runnable onSuccess, Consumer<Throwable> onFailure);

    void enqueueForTellNext(JnksIotMsg msg, Set<String> relationTypes, Runnable onSuccess, Consumer<Throwable> onFailure);

    void enqueueForTellNext(JnksIotMsg msg, String queueName, String relationType, Runnable onSuccess, Consumer<Throwable> onFailure);

    void enqueueForTellNext(JnksIotMsg msg, String queueName, Set<String> relationTypes, Runnable onSuccess, Consumer<Throwable> onFailure);

    void ack(JnksIotMsg jnksIotMsg);

    /**
     * Creates a new JnksIotMsg instance with the specified parameters.
     *
     * <p><strong>Deprecated:</strong> This method is deprecated since version 3.6.0 and should only be used when you need to
     * specify a custom message type that doesn't exist in the {@link JnksIotMsgType} enum. For standard message types,
     * it is recommended to use the {@link #newMsg(String, JnksIotMsgType, EntityId, CustomerId, JnksIotMsgMetaData, String)}
     * method instead.</p>
     *
     * @param queueName   the name of the queue where the message will be sent
     * @param type        the type of the message
     * @param originator  the originator of the message
     * @param customerId  the ID of the customer associated with the message
     * @param metaData    the metadata of the message
     * @param data        the data of the message
     * @return new JnksIotMsg instance
     */
    @Deprecated(since = "3.6.0")
    JnksIotMsg newMsg(String queueName, String type, EntityId originator, CustomerId customerId, JnksIotMsgMetaData metaData, String data);

    @Deprecated(since = "3.6.0", forRemoval = true)
    JnksIotMsg transformMsg(JnksIotMsg origMsg, String type, EntityId originator, JnksIotMsgMetaData metaData, String data);

    JnksIotMsg newMsg(String queueName, JnksIotMsgType type, EntityId originator, JnksIotMsgMetaData metaData, String data);

    JnksIotMsg newMsg(String queueName, JnksIotMsgType type, EntityId originator, CustomerId customerId, JnksIotMsgMetaData metaData, String data);

    JnksIotMsg transformMsg(JnksIotMsg origMsg, JnksIotMsgType type, EntityId originator, JnksIotMsgMetaData metaData, String data);

    JnksIotMsg transformMsg(JnksIotMsg origMsg, JnksIotMsgMetaData metaData, String data);

    JnksIotMsg transformMsgOriginator(JnksIotMsg origMsg, EntityId originator);

    JnksIotMsg customerCreatedMsg(Customer customer, RuleNodeId ruleNodeId);

    JnksIotMsg deviceCreatedMsg(Device device, RuleNodeId ruleNodeId);

    JnksIotMsg assetCreatedMsg(Asset asset, RuleNodeId ruleNodeId);

    @Deprecated(since = "3.6.0", forRemoval = true)
    JnksIotMsg alarmActionMsg(Alarm alarm, RuleNodeId ruleNodeId, String action);

    JnksIotMsg alarmActionMsg(Alarm alarm, RuleNodeId ruleNodeId, JnksIotMsgType actionMsgType);

    JnksIotMsg attributesUpdatedActionMsg(EntityId originator, RuleNodeId ruleNodeId, String scope, List<AttributeKvEntry> attributes);

    JnksIotMsg attributesDeletedActionMsg(EntityId originator, RuleNodeId ruleNodeId, String scope, List<String> keys);

    /*
     *
     *  METHODS TO PROCESS THE MESSAGES
     *
     */

    void schedule(Runnable runnable, long delay, TimeUnit timeUnit);

    void checkTenantEntity(EntityId entityId) throws JnksIotNodeException;

    boolean isLocalEntity(EntityId entityId);

    RuleNodeId getSelfId();

    RuleNode getSelf();

    String getRuleChainName();

    String getQueueName();

    TenantId getTenantId();

    AttributesService getAttributesService();

    CustomerService getCustomerService();

    TenantService getTenantService();

    UserService getUserService();

    AssetService getAssetService();

    DeviceService getDeviceService();

    DeviceProfileService getDeviceProfileService();

    AssetProfileService getAssetProfileService();

    DeviceCredentialsService getDeviceCredentialsService();

    DeviceStateManager getDeviceStateManager();

    String getDeviceStateNodeRateLimitConfig();

    JnksIotClusterService getClusterService();

    DashboardService getDashboardService();

    RuleEngineAlarmService getAlarmService();

    AlarmCommentService getAlarmCommentService();

    RuleChainService getRuleChainService();

    RuleEngineRpcService getRpcService();

    RuleEngineTelemetryService getTelemetryService();

    TimeseriesService getTimeseriesService();

    RelationService getRelationService();

    EntityViewService getEntityViewService();

    ResourceService getResourceService();

    OtaPackageService getOtaPackageService();

    RuleEngineDeviceProfileCache getDeviceProfileCache();

    RuleEngineAssetProfileCache getAssetProfileCache();

    QueueService getQueueService();

    QueueStatsService getQueueStatsService();

    ListeningExecutor getMailExecutor();

    ListeningExecutor getSmsExecutor();

    ListeningExecutor getDbCallbackExecutor();

    ListeningExecutor getExternalCallExecutor();

    ListeningExecutor getNotificationExecutor();

    ExecutorProvider getPubSubRuleNodeExecutorProvider();

    MailService getMailService(boolean isSystem);

    SmsService getSmsService();

    SmsSenderFactory getSmsSenderFactory();

    NotificationCenter getNotificationCenter();

    NotificationTargetService getNotificationTargetService();

    NotificationTemplateService getNotificationTemplateService();

    NotificationRequestService getNotificationRequestService();

    NotificationRuleService getNotificationRuleService();

    OAuth2ClientService getOAuth2ClientService();

    DomainService getDomainService();

    MobileAppService getMobileAppService();

    MobileAppBundleService getMobileAppBundleService();

    SlackService getSlackService();

    CalculatedFieldService getCalculatedFieldService();

    RuleEngineCalculatedFieldQueueService getCalculatedFieldQueueService();

    boolean isExternalNodeForceAck();

    /**
     * Creates JS Script Engine
     * @deprecated
     * <p> Use {@link #createScriptEngine} instead.
     *
     */
    @Deprecated
    ScriptEngine createJsScriptEngine(String script, String... argNames);

    ScriptEngine createScriptEngine(ScriptLanguage scriptLang, String script, String... argNames);

    String getServiceId();

    EventLoopGroup getSharedEventLoop();

    CassandraCluster getCassandraCluster();

    JnksIotResultSetFuture submitCassandraReadTask(CassandraStatementTask task);

    JnksIotResultSetFuture submitCassandraWriteTask(CassandraStatementTask task);

    PageData<RuleNodeState> findRuleNodeStates(PageLink pageLink);

    RuleNodeState findRuleNodeStateForEntity(EntityId entityId);

    void removeRuleNodeStateForEntity(EntityId entityId);

    RuleNodeState saveRuleNodeState(RuleNodeState state);

    void clearRuleNodeStates();

    void addTenantProfileListener(Consumer<TenantProfile> listener);

    void addDeviceProfileListeners(Consumer<DeviceProfile> listener, BiConsumer<DeviceId, DeviceProfile> deviceListener);

    void addAssetProfileListeners(Consumer<AssetProfile> listener, BiConsumer<AssetId, AssetProfile> assetListener);

    void removeListeners();

    TenantProfile getTenantProfile();

    WidgetsBundleService getWidgetBundleService();

    WidgetTypeService getWidgetTypeService();

    RuleEngineApiUsageStateService getRuleEngineApiUsageStateService();

    EntityService getEntityService();

    EventService getEventService();

    AuditLogService getAuditLogService();
}
