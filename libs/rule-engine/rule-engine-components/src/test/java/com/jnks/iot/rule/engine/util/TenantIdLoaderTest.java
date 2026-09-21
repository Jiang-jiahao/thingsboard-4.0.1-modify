package com.jnks.iot.rule.engine.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.AbstractListeningExecutor;
import com.jnks.iot.rule.engine.api.RuleEngineAlarmService;
import com.jnks.iot.rule.engine.api.RuleEngineApiUsageStateService;
import com.jnks.iot.rule.engine.api.RuleEngineAssetProfileCache;
import com.jnks.iot.rule.engine.api.RuleEngineDeviceProfileCache;
import com.jnks.iot.rule.engine.api.RuleEngineRpcService;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.server.common.data.ApiUsageState;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.Dashboard;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.EntityView;
import com.jnks.iot.server.common.data.OtaPackage;
import com.jnks.iot.server.common.data.JnksIotResource;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.asset.Asset;
import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.cf.CalculatedField;
import com.jnks.iot.server.common.data.cf.CalculatedFieldLink;
import com.jnks.iot.server.common.data.domain.Domain;
import com.jnks.iot.server.common.data.id.AssetProfileId;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.id.NotificationId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.TenantProfileId;
import com.jnks.iot.server.common.data.mobile.app.MobileApp;
import com.jnks.iot.server.common.data.mobile.bundle.MobileAppBundle;
import com.jnks.iot.server.common.data.notification.NotificationRequest;
import com.jnks.iot.server.common.data.notification.rule.NotificationRule;
import com.jnks.iot.server.common.data.notification.targets.NotificationTarget;
import com.jnks.iot.server.common.data.notification.template.NotificationTemplate;
import com.jnks.iot.server.common.data.oauth2.OAuth2Client;
import com.jnks.iot.server.common.data.queue.Queue;
import com.jnks.iot.server.common.data.queue.QueueStats;
import com.jnks.iot.server.common.data.rpc.Rpc;
import com.jnks.iot.server.common.data.rule.RuleChain;
import com.jnks.iot.server.common.data.rule.RuleNode;
import com.jnks.iot.server.common.data.widget.WidgetType;
import com.jnks.iot.server.common.data.widget.WidgetsBundle;
import com.jnks.iot.server.dao.asset.AssetService;
import com.jnks.iot.server.dao.cf.CalculatedFieldService;
import com.jnks.iot.server.dao.customer.CustomerService;
import com.jnks.iot.server.dao.dashboard.DashboardService;
import com.jnks.iot.server.dao.device.DeviceService;
import com.jnks.iot.server.dao.domain.DomainService;
import com.jnks.iot.server.dao.entityview.EntityViewService;
import com.jnks.iot.server.dao.mobile.MobileAppBundleService;
import com.jnks.iot.server.dao.mobile.MobileAppService;
import com.jnks.iot.server.dao.notification.NotificationRequestService;
import com.jnks.iot.server.dao.notification.NotificationRuleService;
import com.jnks.iot.server.dao.notification.NotificationTargetService;
import com.jnks.iot.server.dao.notification.NotificationTemplateService;
import com.jnks.iot.server.dao.oauth2.OAuth2ClientService;
import com.jnks.iot.server.dao.ota.OtaPackageService;
import com.jnks.iot.server.dao.queue.QueueService;
import com.jnks.iot.server.dao.queue.QueueStatsService;
import com.jnks.iot.server.dao.resource.ResourceService;
import com.jnks.iot.server.dao.rule.RuleChainService;
import com.jnks.iot.server.dao.user.UserService;
import com.jnks.iot.server.dao.widget.WidgetTypeService;
import com.jnks.iot.server.dao.widget.WidgetsBundleService;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TenantIdLoaderTest {

    @Mock
    private JnksIotContext ctx;
    @Mock
    private CustomerService customerService;
    @Mock
    private UserService userService;
    @Mock
    private AssetService assetService;
    @Mock
    private DeviceService deviceService;
    @Mock
    private RuleEngineAlarmService alarmService;
    @Mock
    private RuleChainService ruleChainService;
    @Mock
    private EntityViewService entityViewService;
    @Mock
    private DashboardService dashboardService;
    @Mock
    private OtaPackageService otaPackageService;
    @Mock
    private RuleEngineAssetProfileCache assetProfileCache;
    @Mock
    private RuleEngineDeviceProfileCache deviceProfileCache;
    @Mock
    private WidgetTypeService widgetTypeService;
    @Mock
    private WidgetsBundleService widgetsBundleService;
    @Mock
    private QueueService queueService;
    @Mock
    private ResourceService resourceService;
    @Mock
    private RuleEngineRpcService rpcService;
    @Mock
    private RuleEngineApiUsageStateService ruleEngineApiUsageStateService;
    @Mock
    private NotificationTargetService notificationTargetService;
    @Mock
    private NotificationTemplateService notificationTemplateService;
    @Mock
    private NotificationRequestService notificationRequestService;
    @Mock
    private NotificationRuleService notificationRuleService;
    @Mock
    private QueueStatsService queueStatsService;
    @Mock
    private OAuth2ClientService oAuth2ClientService;
    @Mock
    private DomainService domainService;
    @Mock
    private MobileAppService mobileAppService;
    @Mock
    private MobileAppBundleService mobileAppBundleService;
    @Mock
    private CalculatedFieldService calculatedFieldService;

    private TenantId tenantId;
    private TenantProfileId tenantProfileId;
    private NotificationId notificationId;
    private AbstractListeningExecutor dbExecutor;

    @BeforeEach
    public void before() {
        dbExecutor = new AbstractListeningExecutor() {
            @Override
            protected int getThreadPollSize() {
                return 3;
            }
        };
        dbExecutor.init();
        this.tenantId = new TenantId(UUID.randomUUID());
        this.tenantProfileId = new TenantProfileId(UUID.randomUUID());
        this.notificationId = new NotificationId(UUID.randomUUID());

        when(ctx.getTenantId()).thenReturn(tenantId);

        for (EntityType entityType : EntityType.values()) {
            initMocks(entityType, tenantId);
        }
    }

    @AfterEach
    public void after() {
        dbExecutor.destroy();
    }

    private void initMocks(EntityType entityType, TenantId tenantId) {
        switch (entityType) {
            case TENANT:
            case NOTIFICATION:
                break;
            case CUSTOMER:
                Customer customer = new Customer();
                customer.setTenantId(tenantId);

                when(ctx.getCustomerService()).thenReturn(customerService);
                doReturn(customer).when(customerService).findCustomerById(eq(tenantId), any());

                break;
            case USER:
                User user = new User();
                user.setTenantId(tenantId);

                when(ctx.getUserService()).thenReturn(userService);
                doReturn(user).when(userService).findUserById(eq(tenantId), any());

                break;
            case ASSET:
                Asset asset = new Asset();
                asset.setTenantId(tenantId);

                when(ctx.getAssetService()).thenReturn(assetService);
                doReturn(asset).when(assetService).findAssetById(eq(tenantId), any());

                break;
            case DEVICE:
                Device device = new Device();
                device.setTenantId(tenantId);

                when(ctx.getDeviceService()).thenReturn(deviceService);
                doReturn(device).when(deviceService).findDeviceById(eq(tenantId), any());

                break;
            case ALARM:
                Alarm alarm = new Alarm();
                alarm.setTenantId(tenantId);

                when(ctx.getAlarmService()).thenReturn(alarmService);
                doReturn(alarm).when(alarmService).findAlarmById(eq(tenantId), any());

                break;
            case RULE_CHAIN:
                RuleChain ruleChain = new RuleChain();
                ruleChain.setTenantId(tenantId);

                when(ctx.getRuleChainService()).thenReturn(ruleChainService);
                doReturn(ruleChain).when(ruleChainService).findRuleChainById(eq(tenantId), any());

                break;
            case ENTITY_VIEW:
                EntityView entityView = new EntityView();
                entityView.setTenantId(tenantId);

                when(ctx.getEntityViewService()).thenReturn(entityViewService);
                doReturn(entityView).when(entityViewService).findEntityViewById(eq(tenantId), any());

                break;
            case DASHBOARD:
                Dashboard dashboard = new Dashboard();
                dashboard.setTenantId(tenantId);

                when(ctx.getDashboardService()).thenReturn(dashboardService);
                doReturn(dashboard).when(dashboardService).findDashboardById(eq(tenantId), any());

                break;
            case OTA_PACKAGE:
                OtaPackage otaPackage = new OtaPackage();
                otaPackage.setTenantId(tenantId);

                when(ctx.getOtaPackageService()).thenReturn(otaPackageService);
                doReturn(otaPackage).when(otaPackageService).findOtaPackageInfoById(eq(tenantId), any());

                break;
            case ASSET_PROFILE:
                AssetProfile assetProfile = new AssetProfile();
                assetProfile.setTenantId(tenantId);

                when(ctx.getAssetProfileCache()).thenReturn(assetProfileCache);
                doReturn(assetProfile).when(assetProfileCache).get(eq(tenantId), any(AssetProfileId.class));

                break;
            case DEVICE_PROFILE:
                DeviceProfile deviceProfile = new DeviceProfile();
                deviceProfile.setTenantId(tenantId);

                when(ctx.getDeviceProfileCache()).thenReturn(deviceProfileCache);
                doReturn(deviceProfile).when(deviceProfileCache).get(eq(tenantId), any(DeviceProfileId.class));

                break;
            case WIDGET_TYPE:
                WidgetType widgetType = new WidgetType();
                widgetType.setTenantId(tenantId);

                when(ctx.getWidgetTypeService()).thenReturn(widgetTypeService);
                doReturn(widgetType).when(widgetTypeService).findWidgetTypeById(eq(tenantId), any());

                break;
            case WIDGETS_BUNDLE:
                WidgetsBundle widgetsBundle = new WidgetsBundle();
                widgetsBundle.setTenantId(tenantId);

                when(ctx.getWidgetBundleService()).thenReturn(widgetsBundleService);
                doReturn(widgetsBundle).when(widgetsBundleService).findWidgetsBundleById(eq(tenantId), any());

                break;
            case RPC:
                Rpc rpc = new Rpc();
                rpc.setTenantId(tenantId);

                when(ctx.getRpcService()).thenReturn(rpcService);
                doReturn(rpc).when(rpcService).findRpcById(eq(tenantId), any());

                break;
            case QUEUE:
                Queue queue = new Queue();
                queue.setTenantId(tenantId);

                when(ctx.getQueueService()).thenReturn(queueService);
                doReturn(queue).when(queueService).findQueueById(eq(tenantId), any());

                break;
            case API_USAGE_STATE:
                ApiUsageState apiUsageState = new ApiUsageState();
                apiUsageState.setTenantId(tenantId);

                when(ctx.getRuleEngineApiUsageStateService()).thenReturn(ruleEngineApiUsageStateService);
                doReturn(apiUsageState).when(ruleEngineApiUsageStateService).findApiUsageStateById(eq(tenantId), any());

                break;
            case JNKS_IOT_RESOURCE:
                JnksIotResource jnksIotResource = new JnksIotResource();
                jnksIotResource.setTenantId(tenantId);

                when(ctx.getResourceService()).thenReturn(resourceService);
                doReturn(jnksIotResource).when(resourceService).findResourceInfoById(eq(tenantId), any());

                break;
            case RULE_NODE:
                RuleNode ruleNode = new RuleNode();

                when(ctx.getRuleChainService()).thenReturn(ruleChainService);
                doReturn(ruleNode).when(ruleChainService).findRuleNodeById(eq(tenantId), any());

                break;
            case TENANT_PROFILE:
                TenantProfile tenantProfile = new TenantProfile(tenantProfileId);

                when(ctx.getTenantProfile()).thenReturn(tenantProfile);

                break;
            case NOTIFICATION_TARGET:
                NotificationTarget notificationTarget = new NotificationTarget();
                notificationTarget.setTenantId(tenantId);
                when(ctx.getNotificationTargetService()).thenReturn(notificationTargetService);
                doReturn(notificationTarget).when(notificationTargetService).findNotificationTargetById(eq(tenantId), any());
                break;
            case NOTIFICATION_TEMPLATE:
                NotificationTemplate notificationTemplate = new NotificationTemplate();
                notificationTemplate.setTenantId(tenantId);
                when(ctx.getNotificationTemplateService()).thenReturn(notificationTemplateService);
                doReturn(notificationTemplate).when(notificationTemplateService).findNotificationTemplateById(eq(tenantId), any());
                break;
            case NOTIFICATION_REQUEST:
                NotificationRequest notificationRequest = new NotificationRequest();
                notificationRequest.setTenantId(tenantId);
                when(ctx.getNotificationRequestService()).thenReturn(notificationRequestService);
                doReturn(notificationRequest).when(notificationRequestService).findNotificationRequestById(eq(tenantId), any());
                break;
            case NOTIFICATION_RULE:
                NotificationRule notificationRule = new NotificationRule();
                notificationRule.setTenantId(tenantId);
                when(ctx.getNotificationRuleService()).thenReturn(notificationRuleService);
                doReturn(notificationRule).when(notificationRuleService).findNotificationRuleById(eq(tenantId), any());
                break;
            case QUEUE_STATS:
                QueueStats queueStats = new QueueStats();
                queueStats.setTenantId(tenantId);
                when(ctx.getQueueStatsService()).thenReturn(queueStatsService);
                doReturn(queueStats).when(queueStatsService).findQueueStatsById(eq(tenantId), any());
                break;
            case OAUTH2_CLIENT:
                OAuth2Client oAuth2Client = new OAuth2Client();
                oAuth2Client.setTenantId(tenantId);
                when(ctx.getOAuth2ClientService()).thenReturn(oAuth2ClientService);
                doReturn(oAuth2Client).when(oAuth2ClientService).findOAuth2ClientById(eq(tenantId), any());
                break;
            case DOMAIN:
                Domain domain = new Domain();
                domain.setTenantId(tenantId);
                when(ctx.getDomainService()).thenReturn(domainService);
                doReturn(domain).when(domainService).findDomainById(eq(tenantId), any());
                break;
            case MOBILE_APP:
                MobileApp mobileApp = new MobileApp();
                mobileApp.setTenantId(tenantId);
                when(ctx.getMobileAppService()).thenReturn(mobileAppService);
                doReturn(mobileApp).when(mobileAppService).findMobileAppById(eq(tenantId), any());
                break;
            case MOBILE_APP_BUNDLE:
                MobileAppBundle mobileAppBundle = new MobileAppBundle();
                mobileAppBundle.setTenantId(tenantId);
                when(ctx.getMobileAppBundleService()).thenReturn(mobileAppBundleService);
                doReturn(mobileAppBundle).when(mobileAppBundleService).findMobileAppBundleById(eq(tenantId), any());
                break;
            case CALCULATED_FIELD:
                CalculatedField calculatedField = new CalculatedField();
                calculatedField.setTenantId(tenantId);
                when(ctx.getCalculatedFieldService()).thenReturn(calculatedFieldService);
                doReturn(calculatedField).when(calculatedFieldService).findById(eq(tenantId), any());
                break;
            case CALCULATED_FIELD_LINK:
                CalculatedFieldLink calculatedFieldLink = new CalculatedFieldLink();
                calculatedFieldLink.setTenantId(tenantId);
                when(ctx.getCalculatedFieldService()).thenReturn(calculatedFieldService);
                doReturn(calculatedFieldLink).when(calculatedFieldService).findCalculatedFieldLinkById(eq(tenantId), any());
                break;
            default:
                throw new RuntimeException("Unexpected originator EntityType " + entityType);
        }
    }

    private EntityId getEntityId(EntityType entityType) {
        return EntityIdFactory.getByTypeAndUuid(entityType, UUID.randomUUID());
    }

    private void checkTenant(TenantId checkTenantId, boolean equals) {
        for (EntityType entityType : EntityType.values()) {
            EntityId entityId;
            if (EntityType.TENANT.equals(entityType)) {
                entityId = tenantId;
            } else if (EntityType.TENANT_PROFILE.equals(entityType)) {
                entityId = tenantProfileId;
            } else {
                entityId = getEntityId(entityType);
            }
            TenantId targetTenantId = TenantIdLoader.findTenantId(ctx, entityId);
            String msg = "Check entity type <" + entityType.name() + ">:";
            if (equals) {
                Assertions.assertEquals(targetTenantId, checkTenantId, msg);
            } else {
                Assertions.assertNotEquals(targetTenantId, checkTenantId, msg);
            }
        }
    }

    @Test
    public void test_findEntityIdAsync_current_tenant() {
        checkTenant(tenantId, true);
    }

    @Test
    public void test_findEntityIdAsync_other_tenant() {
        checkTenant(new TenantId(UUID.randomUUID()), false);
    }

}
