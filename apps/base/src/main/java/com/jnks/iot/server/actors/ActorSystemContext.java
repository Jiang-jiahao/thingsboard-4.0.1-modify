package com.jnks.iot.server.actors;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import com.jnks.iot.common.util.ExecutorProvider;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.DeviceStateManager;
import com.jnks.iot.rule.engine.api.MailService;
import com.jnks.iot.rule.engine.api.NotificationCenter;
import com.jnks.iot.rule.engine.api.SmsService;
import com.jnks.iot.rule.engine.api.notification.SlackService;
import com.jnks.iot.rule.engine.api.sms.SmsSenderFactory;
import com.jnks.iot.script.api.js.JsInvokeService;
import com.jnks.iot.script.api.tbel.TbelInvokeService;
import com.jnks.iot.server.actors.service.ActorService;
import com.jnks.iot.server.actors.tenant.TenantDeviceActorSupport;
import com.jnks.iot.server.actors.tenant.TenantRuleEngineActorSupportFactory;
import com.jnks.iot.server.actors.tenant.DebugJnksIotRateLimits;
import com.jnks.iot.server.cache.limits.RateLimitService;
import com.jnks.iot.server.cluster.JnksIotClusterService;
import com.jnks.iot.server.common.data.event.CalculatedFieldDebugEvent;
import com.jnks.iot.server.common.data.event.ErrorEvent;
import com.jnks.iot.server.common.data.event.LifecycleEvent;
import com.jnks.iot.server.common.data.event.RuleChainDebugEvent;
import com.jnks.iot.server.common.data.event.RuleNodeDebugEvent;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.limit.LimitedApi;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleEvent;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.notification.NotificationRuleProcessor;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.common.msg.tools.JnksIotRateLimits;
import com.jnks.iot.server.common.stats.JnksIotApiUsageReportClient;
import com.jnks.iot.server.dao.alarm.AlarmCommentService;
import com.jnks.iot.server.dao.asset.AssetProfileService;
import com.jnks.iot.server.dao.asset.AssetService;
import com.jnks.iot.server.dao.attributes.AttributesService;
import com.jnks.iot.server.dao.audit.AuditLogService;
import com.jnks.iot.server.dao.cassandra.CassandraCluster;
import com.jnks.iot.server.dao.cf.CalculatedFieldService;
import com.jnks.iot.server.dao.customer.CustomerService;
import com.jnks.iot.server.dao.dashboard.DashboardService;
import com.jnks.iot.server.dao.device.ClaimDevicesService;
import com.jnks.iot.server.dao.device.DeviceCredentialsService;
import com.jnks.iot.server.dao.device.DeviceProfileService;
import com.jnks.iot.server.dao.device.DeviceService;
import com.jnks.iot.server.dao.domain.DomainService;
import com.jnks.iot.server.dao.entity.EntityService;
import com.jnks.iot.server.dao.entityview.EntityViewService;
import com.jnks.iot.server.dao.event.EventService;
import com.jnks.iot.server.dao.mobile.MobileAppBundleService;
import com.jnks.iot.server.dao.mobile.MobileAppService;
import com.jnks.iot.server.dao.nosql.CassandraBufferedRateReadExecutor;
import com.jnks.iot.server.dao.nosql.CassandraBufferedRateWriteExecutor;
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
import com.jnks.iot.server.dao.rule.RuleNodeStateService;
import com.jnks.iot.server.dao.tenant.JnksIotTenantProfileCache;
import com.jnks.iot.server.dao.tenant.TenantProfileService;
import com.jnks.iot.server.dao.tenant.TenantService;
import com.jnks.iot.server.dao.timeseries.TimeseriesService;
import com.jnks.iot.server.dao.usagerecord.ApiLimitService;
import com.jnks.iot.server.dao.user.UserService;
import com.jnks.iot.server.dao.widget.WidgetTypeService;
import com.jnks.iot.server.dao.widget.WidgetsBundleService;
import com.jnks.iot.server.queue.discovery.DiscoveryService;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.queue.discovery.JnksIotServiceInfoProvider;
import com.jnks.iot.server.queue.settings.JnksIotQueueCalculatedFieldSettings;
import com.jnks.iot.server.service.apiusage.JnksIotApiUsageStateService;
import com.jnks.iot.server.service.cf.CalculatedFieldQueueService;
import com.jnks.iot.server.service.component.ComponentDiscoveryService;
import com.jnks.iot.server.service.entitiy.entityview.JnksIotEntityViewService;
import com.jnks.iot.server.service.executors.DbCallbackExecutorService;
import com.jnks.iot.server.service.executors.ExternalCallExecutorService;
import com.jnks.iot.server.service.executors.NotificationExecutorService;
import com.jnks.iot.server.service.executors.SharedEventLoopGroupService;
import com.jnks.iot.server.service.mail.MailExecutorService;
import com.jnks.iot.server.service.profile.JnksIotAssetProfileCache;
import com.jnks.iot.server.service.profile.JnksIotDeviceProfileCache;
import com.jnks.iot.server.service.rpc.JnksIotCoreDeviceRpcService;
import com.jnks.iot.server.service.rpc.JnksIotRpcService;
import com.jnks.iot.server.service.rpc.JnksIotRuleEngineDeviceRpcService;
import com.jnks.iot.server.service.session.DeviceSessionCacheService;
import com.jnks.iot.server.service.sms.SmsExecutorService;
import com.jnks.iot.server.service.state.DeviceStateService;
import com.jnks.iot.server.service.telemetry.AlarmSubscriptionService;
import com.jnks.iot.server.service.telemetry.TelemetrySubscriptionService;
import com.jnks.iot.server.service.transport.JnksIotCoreToTransportService;
import com.jnks.iot.server.utils.DebugModeRateLimitsConfig;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * actor缂侇垵宕电划鐑樼▔婵犱胶鐟撻柡鍌氭祫缁辨繈鏌屽畝鍕〃闁哄牆顦花鍝勨柦閳╁啯绠掑ù锝堟硶閺併倝宕氶幍顔界暠缂備礁瀚▎銏＄▔閺勫浚娲ｉ柡鍕靛灟鐠愮喐绋婄€ｎ亝鍊甸柟纰樻櫅閻秵鎷呯捄銊︽殢
 * 婵絽绻嬮柌娓乧tor濞戞挸绉瑰〒鍓佹啺娴ｇǹ绠垫繛澶堝妼閸欏棝宕ラ崟顓ф綒闁汇劌瀚悮顐︽晬鐏炶棄娑ч梻鍥ｅ亾閻熸洑妞掔槐鍫曞礂閵夈倗鐟愬☉鎾愁儐閺嬪啴鏁嶅畝鍐ㄧ闁告瑦鐗旂粭鍌涚▔鐎ｎ偅鐎☉鎿冨幘濞堟垹绱掗崟顏咁偨閻庣數顢婇挅鍕础閸愭彃璁�
 */
@Slf4j
@Component
public class ActorSystemContext {

    private static final FutureCallback<Void> RULE_CHAIN_DEBUG_EVENT_ERROR_CALLBACK = new FutureCallback<>() {
        @Override
        public void onSuccess(@Nullable Void event) {

        }

        @Override
        public void onFailure(Throwable th) {
            log.error("Could not save debug Event for Rule Chain", th);
        }
    };
    private static final FutureCallback<Void> RULE_NODE_DEBUG_EVENT_ERROR_CALLBACK = new FutureCallback<>() {
        @Override
        public void onSuccess(@Nullable Void event) {

        }

        @Override
        public void onFailure(Throwable th) {
            log.error("Could not save debug Event for Node", th);
        }
    };

    private static final FutureCallback<Void> CALCULATED_FIELD_DEBUG_EVENT_ERROR_CALLBACK = new FutureCallback<>() {
        @Override
        public void onSuccess(@Nullable Void event) {

        }

        @Override
        public void onFailure(Throwable th) {
            log.error("Could not save debug Event for Calculated Field", th);
        }
    };

    private final ConcurrentMap<TenantId, DebugJnksIotRateLimits> debugPerTenantLimits = new ConcurrentHashMap<>();

    public ConcurrentMap<TenantId, DebugJnksIotRateLimits> getDebugPerTenantLimits() {
        return debugPerTenantLimits;
    }

    @Autowired
    @Getter
    private JnksIotApiUsageStateService apiUsageStateService;

    @Autowired
    @Getter
    private JnksIotApiUsageReportClient apiUsageClient;

    @Autowired
    @Getter
    @Setter
    private JnksIotServiceInfoProvider serviceInfoProvider;

    @Getter
    @Setter
    private ActorService actorService;

    @Autowired
    @Getter
    @Setter
    private ComponentDiscoveryService componentService;

    @Autowired
    @Getter
    private DiscoveryService discoveryService;

    @Autowired
    @Getter
    private DeviceService deviceService;

    @Autowired
    @Getter
    private DeviceProfileService deviceProfileService;

    @Autowired
    @Getter
    private AssetProfileService assetProfileService;

    @Autowired
    @Getter
    private DeviceCredentialsService deviceCredentialsService;

    @Autowired(required = false)
    @Getter
    private DeviceStateManager deviceStateManager;

    @Autowired
    @Getter
    private JnksIotTenantProfileCache tenantProfileCache;

    @Autowired
    @Getter
    private JnksIotDeviceProfileCache deviceProfileCache;

    @Autowired
    @Getter
    private JnksIotAssetProfileCache assetProfileCache;

    @Autowired
    @Getter
    private AssetService assetService;

    @Autowired
    @Getter
    private DashboardService dashboardService;

    @Autowired
    @Getter
    private TenantService tenantService;

    @Autowired
    @Getter
    private TenantProfileService tenantProfileService;

    @Autowired
    @Getter
    private CustomerService customerService;

    @Autowired
    @Getter
    private UserService userService;

    @Autowired
    @Getter
    private RuleChainService ruleChainService;

    @Autowired
    @Getter
    private RuleNodeStateService ruleNodeStateService;

    @Autowired
    @Getter
    private PartitionService partitionService;

    @Autowired
    @Getter
    private JnksIotClusterService clusterService;

    @Autowired
    @Getter
    private TimeseriesService tsService;

    @Autowired
    @Getter
    private AttributesService attributesService;

    @Autowired
    @Getter
    private EventService eventService;

    @Autowired
    @Getter
    private RelationService relationService;

    @Autowired
    @Getter
    private AuditLogService auditLogService;

    @Autowired
    @Getter
    private EntityViewService entityViewService;

    @Lazy
    @Autowired(required = false)
    @Getter
    private JnksIotEntityViewService jnksIotEntityViewService;

    @Lazy
    @Autowired
    @Getter
    private TelemetrySubscriptionService tsSubService;

    @Autowired
    @Getter
    private AlarmSubscriptionService alarmService;

    @Autowired
    @Getter
    private AlarmCommentService alarmCommentService;

    @Autowired
    @Getter
    private JsInvokeService jsInvokeService;

    @Autowired(required = false)
    @Getter
    private TbelInvokeService tbelInvokeService;

    @Autowired
    @Getter
    private MailExecutorService mailExecutor;

    @Autowired
    @Getter
    private SmsExecutorService smsExecutor;

    @Autowired
    @Getter
    private DbCallbackExecutorService dbCallbackExecutor;

    @Autowired
    @Getter
    private ExternalCallExecutorService externalCallExecutorService;

    @Autowired
    @Getter
    private NotificationExecutorService notificationExecutor;

    @Lazy
    @Autowired
    @Getter
    private ExecutorProvider pubSubRuleNodeExecutorProvider;

    @Autowired
    @Getter
    private SharedEventLoopGroupService sharedEventLoopGroupService;

    @Autowired
    @Getter
    private MailService mailService;

    @Autowired
    @Getter
    private SmsService smsService;

    @Autowired
    @Getter
    private SmsSenderFactory smsSenderFactory;

    @Autowired
    @Getter
    private NotificationCenter notificationCenter;

    @Autowired(required = false)
    @Getter
    private NotificationRuleProcessor notificationRuleProcessor;

    @Autowired
    @Getter
    private NotificationTargetService notificationTargetService;

    @Autowired
    @Getter
    private NotificationTemplateService notificationTemplateService;

    @Autowired
    @Getter
    private NotificationRequestService notificationRequestService;

    @Autowired
    @Getter
    private NotificationRuleService notificationRuleService;

    @Autowired
    @Getter
    private OAuth2ClientService oAuth2ClientService;

    @Autowired
    @Getter
    private DomainService domainService;

    @Autowired
    @Getter
    private MobileAppService mobileAppService;

    @Autowired
    @Getter
    private MobileAppBundleService mobileAppBundleService;

    @Autowired(required = false)
    @Getter
    private SlackService slackService;

    @Autowired
    @Getter
    private CalculatedFieldService calculatedFieldService;

    @Lazy
    @Autowired(required = false)
    @Getter
    private ClaimDevicesService claimDevicesService;

    //TODO: separate context for JnksIotCore and JnksIotRuleEngine
    @Autowired(required = false)
    @Getter
    private DeviceStateService deviceStateService;

    @Autowired(required = false)
    @Getter
    private DeviceSessionCacheService deviceSessionCacheService;

    @Autowired(required = false)
    @Getter
    private JnksIotCoreToTransportService jnksIotCoreToTransportService;

    @Lazy
    @Autowired(required = false)
    @Getter
    private ApiLimitService apiLimitService;

    @Lazy
    @Autowired(required = false)
    @Getter
    private RateLimitService rateLimitService;

    @Lazy
    @Autowired(required = false)
    @Getter
    private DebugModeRateLimitsConfig debugModeRateLimitsConfig;

    @Lazy
    @Autowired(required = false)
    @Getter
    private JnksIotQueueCalculatedFieldSettings calculatedFieldSettings;

    /**
     * The following Service will be null if we operate in jnks-iot-core mode
     */
    @Lazy
    @Autowired(required = false)
    @Getter
    private JnksIotRuleEngineDeviceRpcService jnksIotRuleEngineDeviceRpcService;

    @Autowired(required = false)
    @Getter
    private TenantDeviceActorSupport tenantDeviceActorSupport;

    @Autowired(required = false)
    @Getter
    private TenantRuleEngineActorSupportFactory tenantRuleEngineActorSupportFactory;

    /**
     * The following Service will be null if we operate in jnks-iot-rule-engine mode
     */
    @Lazy
    @Autowired(required = false)
    @Getter
    private JnksIotCoreDeviceRpcService jnksIotCoreDeviceRpcService;

    @Lazy
    @Autowired(required = false)
    @Getter
    private ResourceService resourceService;

    @Lazy
    @Autowired(required = false)
    @Getter
    private OtaPackageService otaPackageService;

    @Lazy
    @Autowired(required = false)
    @Getter
    private JnksIotRpcService jnksIotRpcService;

    @Lazy
    @Autowired(required = false)
    @Getter
    private QueueService queueService;

    @Lazy
    @Autowired(required = false)
    @Getter
    private QueueStatsService queueStatsService;

    @Lazy
    @Autowired(required = false)
    @Getter
    private WidgetsBundleService widgetsBundleService;

    @Lazy
    @Autowired(required = false)
    @Getter
    private WidgetTypeService widgetTypeService;

    @Lazy
    @Autowired(required = false)
    @Getter
    private EntityService entityService;

    @Autowired(required = false)
    @Qualifier("defaultCalculatedFieldProcessingService")
    private ObjectProvider<Object> calculatedFieldProcessingServiceProvider;

    @Autowired(required = false)
    @Qualifier("kafkaCalculatedFieldStateService")
    private ObjectProvider<Object> kafkaCalculatedFieldStateServiceProvider;

    @Autowired(required = false)
    @Qualifier("rocksDBCalculatedFieldStateService")
    private ObjectProvider<Object> rocksDBCalculatedFieldStateServiceProvider;

    @Lazy
    @Autowired(required = false)
    @Getter
    private CalculatedFieldQueueService calculatedFieldQueueService;

    @Value("${actors.session.max_concurrent_sessions_per_device:1}")
    @Getter
    private int maxConcurrentSessionsPerDevice;

    @Value("${actors.session.sync.timeout:10000}")
    @Getter
    private long syncSessionTimeout;

    @Value("${actors.rule.chain.error_persist_frequency:3000}")
    @Getter
    private long ruleChainErrorPersistFrequency;

    @Value("${actors.rule.node.error_persist_frequency:3000}")
    @Getter
    private long ruleNodeErrorPersistFrequency;

    @Value("${actors.statistics.enabled:true}")
    @Getter
    private boolean statisticsEnabled;

    @Value("${actors.statistics.persist_frequency:3600000}")
    @Getter
    private long statisticsPersistFrequency;

    @Value("${cache.type:caffeine}")
    @Getter
    private String cacheType;

    @Getter
    private boolean localCacheType;

    @PostConstruct
    public void init() {
        this.localCacheType = "caffeine".equals(cacheType);
    }

    @Value("${actors.tenant.create_components_on_init:true}")
    @Getter
    private boolean tenantComponentsInitEnabled;

    @Value("${actors.rule.allow_system_mail_service:true}")
    @Getter
    private boolean allowSystemMailService;

    @Value("${actors.rule.allow_system_sms_service:true}")
    @Getter
    private boolean allowSystemSmsService;

    @Value("${transport.sessions.inactivity_timeout:300000}")
    @Getter
    private long sessionInactivityTimeout;

    @Value("${transport.sessions.report_timeout:3000}")
    @Getter
    private long sessionReportTimeout;

    @Value("${actors.rpc.submit_strategy:BURST}")
    @Getter
    private String rpcSubmitStrategy;

    @Value("${actors.rpc.close_session_on_rpc_delivery_timeout:false}")
    @Getter
    private boolean closeTransportSessionOnRpcDeliveryTimeout;

    @Value("${actors.rpc.response_timeout_ms:30000}")
    @Getter
    private long rpcResponseTimeout;

    @Value("${actors.rpc.max_retries:5}")
    @Getter
    private int maxRpcRetries;

    @Value("${actors.rule.external.force_ack:false}")
    @Getter
    private boolean externalNodeForceAck;

    @Value("${state.rule.node.deviceState.rateLimit:1:1,30:60,60:3600}")
    @Getter
    private String deviceStateNodeRateLimitConfig;

    @Value("${actors.calculated_fields.calculation_timeout:5}")
    @Getter
    private long cfCalculationResultTimeout;

    @Getter
    @Setter
    private JnksIotActorSystem actorSystem;

    @Setter
    private JnksIotActorRef appActor;

    @Getter
    @Setter
    private JnksIotActorRef statsActor;

    @Autowired(required = false)
    @Getter
    private CassandraCluster cassandraCluster;

    @Autowired(required = false)
    @Getter
    private CassandraBufferedRateReadExecutor cassandraBufferedRateReadExecutor;

    @Autowired(required = false)
    @Getter
    private CassandraBufferedRateWriteExecutor cassandraBufferedRateWriteExecutor;

    @Autowired(required = false)
    @Getter
    private RedisTemplate<String, Object> redisTemplate;

    public ScheduledExecutorService getScheduler() {
        return actorSystem.getScheduler();
    }

    public void persistError(TenantId tenantId, EntityId entityId, String method, Exception e) {
        eventService.saveAsync(ErrorEvent.builder()
                .tenantId(tenantId)
                .entityId(entityId.getId())
                .serviceId(getServiceId())
                .method(method)
                .error(toString(e)).build());
    }

    public void persistLifecycleEvent(TenantId tenantId, EntityId entityId, ComponentLifecycleEvent lcEvent, Exception e) {
        LifecycleEvent.LifecycleEventBuilder event = LifecycleEvent.builder()
                .tenantId(tenantId)
                .entityId(entityId.getId())
                .serviceId(getServiceId())
                .lcEventType(lcEvent.name());

        if (e != null) {
            event.success(false).error(toString(e));
        } else {
            event.success(true);
        }

        eventService.saveAsync(event.build());
    }

    private String toString(Throwable e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    public TopicPartitionInfo resolve(ServiceType serviceType, TenantId tenantId, EntityId entityId) {
        return partitionService.resolve(serviceType, tenantId, entityId);
    }

    public TopicPartitionInfo resolve(ServiceType serviceType, String queueName, TenantId tenantId, EntityId entityId) {
        return partitionService.resolve(serviceType, queueName, tenantId, entityId);
    }

    public TopicPartitionInfo resolve(TenantId tenantId, EntityId entityId, JnksIotMsg msg) {
        return partitionService.resolve(ServiceType.JNKS_IOT_RULE_ENGINE, msg.getQueueName(), tenantId, entityId, msg.getPartition());
    }

    public String getServiceId() {
        return serviceInfoProvider.getServiceId();
    }

    public void persistDebugInput(TenantId tenantId, EntityId entityId, JnksIotMsg jnksIotMsg, String relationType) {
        persistDebugAsync(tenantId, entityId, "IN", jnksIotMsg, relationType, null, null);
    }

    public void persistDebugInput(TenantId tenantId, EntityId entityId, JnksIotMsg jnksIotMsg, String relationType, Throwable error) {
        persistDebugAsync(tenantId, entityId, "IN", jnksIotMsg, relationType, error, null);
    }

    public void persistDebugOutput(TenantId tenantId, EntityId entityId, JnksIotMsg jnksIotMsg, String relationType, Throwable error, String failureMessage) {
        persistDebugAsync(tenantId, entityId, "OUT", jnksIotMsg, relationType, error, failureMessage);
    }

    public void persistDebugOutput(TenantId tenantId, EntityId entityId, JnksIotMsg jnksIotMsg, String relationType, Throwable error) {
        persistDebugAsync(tenantId, entityId, "OUT", jnksIotMsg, relationType, error, null);
    }

    public void persistDebugOutput(TenantId tenantId, EntityId entityId, JnksIotMsg jnksIotMsg, String relationType) {
        persistDebugAsync(tenantId, entityId, "OUT", jnksIotMsg, relationType, null, null);
    }

    private void persistDebugAsync(TenantId tenantId, EntityId entityId, String type, JnksIotMsg jnksIotMsg, String relationType, Throwable error, String failureMessage) {
        if (checkLimits(tenantId, jnksIotMsg, error)) {
            try {
                RuleNodeDebugEvent.RuleNodeDebugEventBuilder event = RuleNodeDebugEvent.builder()
                        .tenantId(tenantId)
                        .entityId(entityId.getId())
                        .serviceId(getServiceId())
                        .eventType(type)
                        .eventEntity(jnksIotMsg.getOriginator())
                        .msgId(jnksIotMsg.getId())
                        .msgType(jnksIotMsg.getType())
                        .dataType(jnksIotMsg.getDataType().name())
                        .relationType(relationType)
                        .data(jnksIotMsg.getData())
                        .metadata(JacksonUtil.toString(jnksIotMsg.getMetaData().getData()));

                if (error != null) {
                    event.error(toString(error));
                } else if (failureMessage != null) {
                    event.error(failureMessage);
                }

                ListenableFuture<Void> future = eventService.saveAsync(event.build());
                Futures.addCallback(future, RULE_NODE_DEBUG_EVENT_ERROR_CALLBACK, MoreExecutors.directExecutor());
            } catch (IllegalArgumentException ex) {
                log.warn("Failed to persist rule node debug message", ex);
            }
        }
    }

    private boolean checkLimits(TenantId tenantId, JnksIotMsg jnksIotMsg, Throwable error) {
        if (debugModeRateLimitsConfig.isRuleChainDebugPerTenantLimitsEnabled()) {
            DebugJnksIotRateLimits debugJnksIotRateLimits = debugPerTenantLimits.computeIfAbsent(tenantId, id ->
                    new DebugJnksIotRateLimits(new JnksIotRateLimits(debugModeRateLimitsConfig.getRuleChainDebugPerTenantLimitsConfiguration()), false));

            if (!debugJnksIotRateLimits.getJnksIotRateLimits().tryConsume()) {
                if (!debugJnksIotRateLimits.isRuleChainEventSaved()) {
                    persistRuleChainDebugModeEvent(tenantId, jnksIotMsg.getRuleChainId(), error);
                    debugJnksIotRateLimits.setRuleChainEventSaved(true);
                }
                if (log.isTraceEnabled()) {
                    log.trace("[{}] Tenant level debug mode rate limit detected: {}", tenantId, jnksIotMsg);
                }
                return false;
            }
        }
        return true;
    }

    private void persistRuleChainDebugModeEvent(TenantId tenantId, EntityId entityId, Throwable error) {
        RuleChainDebugEvent.RuleChainDebugEventBuilder event = RuleChainDebugEvent.builder()
                .tenantId(tenantId)
                .entityId(entityId.getId())
                .serviceId(getServiceId())
                .message("Reached debug mode rate limit!");
        if (error != null) {
            event.error(toString(error));
        }

        ListenableFuture<Void> future = eventService.saveAsync(event.build());
        Futures.addCallback(future, RULE_CHAIN_DEBUG_EVENT_ERROR_CALLBACK, MoreExecutors.directExecutor());
    }

    public void persistCalculatedFieldDebugEvent(TenantId tenantId, CalculatedFieldId calculatedFieldId, EntityId entityId, Map<String, ?> arguments, UUID jnksIotMsgId, JnksIotMsgType jnksIotMsgType, String result, String errorMessage) {
        if (checkLimits(tenantId)) {
            try {
                CalculatedFieldDebugEvent.CalculatedFieldDebugEventBuilder eventBuilder = CalculatedFieldDebugEvent.builder()
                        .tenantId(tenantId)
                        .entityId(calculatedFieldId.getId())
                        .serviceId(getServiceId())
                        .calculatedFieldId(calculatedFieldId)
                        .eventEntity(entityId);
                if (jnksIotMsgId != null) {
                    eventBuilder.msgId(jnksIotMsgId);
                }
                if (jnksIotMsgType != null) {
                    eventBuilder.msgType(jnksIotMsgType.name());
                }
                if (arguments != null) {
                    eventBuilder.arguments(JacksonUtil.toString(normalizeCalculatedFieldArguments(arguments)));
                }
                if (result != null) {
                    eventBuilder.result(result);
                }
                if (errorMessage != null) {
                    eventBuilder.error(errorMessage);
                }

                ListenableFuture<Void> future = eventService.saveAsync(eventBuilder.build());
                Futures.addCallback(future, CALCULATED_FIELD_DEBUG_EVENT_ERROR_CALLBACK, MoreExecutors.directExecutor());
            } catch (IllegalArgumentException ex) {
                log.warn("Failed to persist calculated field debug message", ex);
            }
        }
    }

    private Map<String, Object> normalizeCalculatedFieldArguments(Map<String, ?> arguments) {
        Map<String, Object> normalized = new HashMap<>(arguments.size());
        arguments.forEach((key, value) -> normalized.put(key, normalizeCalculatedFieldArgument(value)));
        return normalized;
    }

    private Object normalizeCalculatedFieldArgument(Object value) {
        if (value == null) {
            return null;
        }
        try {
            var toTbelCfArgMethod = value.getClass().getMethod("toTbelCfArg");
            try {
                return toTbelCfArgMethod.invoke(value);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                } else if (cause instanceof Error error) {
                    throw error;
                } else {
                    throw new IllegalArgumentException("Failed to convert calculated field argument using toTbelCfArg()", cause);
                }
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("Failed to convert calculated field argument using toTbelCfArg()", e);
            }
        } catch (NoSuchMethodException e) {
            // Keep backward compatibility when argument is not a calculated-field entry object.
            return value;
        }
    }

    private boolean checkLimits(TenantId tenantId) {
        if (debugModeRateLimitsConfig.isCalculatedFieldDebugPerTenantLimitsEnabled() &&
                !rateLimitService.checkRateLimit(LimitedApi.CALCULATED_FIELD_DEBUG_EVENTS, (Object) tenantId, debugModeRateLimitsConfig.getCalculatedFieldDebugPerTenantLimitsConfiguration())) {
            log.trace("[{}] Calculated field debug event limits exceeded!", tenantId);
            return false;
        }
        return true;
    }

    public static Exception toException(Throwable error) {
        return Exception.class.isInstance(error) ? (Exception) error : new Exception(error);
    }

    public void tell(JnksIotActorMsg jnksIotActorMsg) {
        appActor.tell(jnksIotActorMsg);
    }

    public void tellWithHighPriority(JnksIotActorMsg jnksIotActorMsg) {
        appActor.tellWithHighPriority(jnksIotActorMsg);
    }

    public ScheduledFuture<?> schedulePeriodicMsgWithDelay(JnksIotActorRef ctx, JnksIotActorMsg msg, long delayInMs, long periodInMs) {
        log.debug("Scheduling periodic msg {} every {} ms with delay {} ms", msg, periodInMs, delayInMs);
        return getScheduler().scheduleWithFixedDelay(() -> ctx.tell(msg), delayInMs, periodInMs, TimeUnit.MILLISECONDS);
    }

    public void scheduleMsgWithDelay(JnksIotActorRef ctx, JnksIotActorMsg msg, long delayInMs) {
        log.debug("Scheduling msg {} with delay {} ms", msg, delayInMs);
        if (delayInMs > 0) {
            getScheduler().schedule(() -> ctx.tell(msg), delayInMs, TimeUnit.MILLISECONDS);
        } else {
            ctx.tell(msg);
        }
    }


    public Object getCalculatedFieldProcessingService() {
        return calculatedFieldProcessingServiceProvider.getIfAvailable();
    }
    public Object getCalculatedFieldStateService() {
        Object kafka = kafkaCalculatedFieldStateServiceProvider.getIfAvailable();
        if (kafka != null) {
            return kafka;
        }
        return rocksDBCalculatedFieldStateServiceProvider.getIfAvailable();
    }

}
