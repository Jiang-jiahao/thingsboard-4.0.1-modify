package org.thingsboard.server.common.data.id;

import org.thingsboard.server.common.data.EntityType;

import java.util.UUID;

/**
 * Created by ashvayka on 25.04.17.
 */
public class EntityIdFactory {

    public static EntityId getByTypeAndUuid(int type, String uuid) {
        return getByTypeAndUuid(EntityType.values()[type], UUID.fromString(uuid));
    }

    public static EntityId getByTypeAndUuid(int type, UUID uuid) {
        return getByTypeAndUuid(EntityType.values()[type], uuid);
    }

    public static EntityId getByTypeAndUuid(String type, String uuid) {
        return getByTypeAndUuid(EntityType.valueOf(type), UUID.fromString(uuid));
    }

    public static EntityId getByTypeAndId(String type, String uuid) {
        return getByTypeAndUuid(EntityType.valueOf(type), UUID.fromString(uuid));
    }

    public static EntityId getByTypeAndUuid(String type, UUID uuid) {
        return getByTypeAndUuid(EntityType.valueOf(type), uuid);
    }

    public static EntityId getByTypeAndUuid(EntityType type, String uuid) {
        return getByTypeAndUuid(type, UUID.fromString(uuid));
    }

    public static EntityId getByTypeAndUuid(EntityType type, UUID uuid) {
        switch (type) {
            case TENANT:
                return TenantId.fromUUID(uuid);
            case CUSTOMER:
                return new CustomerId(uuid);
            case USER:
                return new UserId(uuid);
            case DASHBOARD:
                return new DashboardId(uuid);
            case DEVICE:
                return new DeviceId(uuid);
            case ASSET:
                return new AssetId(uuid);
            case ALARM:
                return new AlarmId(uuid);
            case RULE_CHAIN:
                return new RuleChainId(uuid);
            case RULE_NODE:
                return new RuleNodeId(uuid);
            case ENTITY_VIEW:
                return new EntityViewId(uuid);
            case WIDGETS_BUNDLE:
                return new WidgetsBundleId(uuid);
            case WIDGET_TYPE:
                return new WidgetTypeId(uuid);
            case DEVICE_PROFILE:
                return new DeviceProfileId(uuid);
            case ASSET_PROFILE:
                return new AssetProfileId(uuid);
            case TENANT_PROFILE:
                return new TenantProfileId(uuid);
            case API_USAGE_STATE:
                return new ApiUsageStateId(uuid);
            case TB_RESOURCE:
                return new TbResourceId(uuid);
            case OTA_PACKAGE:
                return new OtaPackageId(uuid);
            case RPC:
                return new RpcId(uuid);
            case QUEUE:
                return new QueueId(uuid);
            case NOTIFICATION_TARGET:
                return new NotificationTargetId(uuid);
            case NOTIFICATION_REQUEST:
                return new NotificationRequestId(uuid);
            case NOTIFICATION_RULE:
                return new NotificationRuleId(uuid);
            case NOTIFICATION_TEMPLATE:
                return new NotificationTemplateId(uuid);
            case NOTIFICATION:
                return new NotificationId(uuid);
            case QUEUE_STATS:
                return new QueueStatsId(uuid);
            case OAUTH2_CLIENT:
                return new OAuth2ClientId(uuid);
            case MOBILE_APP:
                return new MobileAppId(uuid);
            case DOMAIN:
                return new DomainId(uuid);
            case MOBILE_APP_BUNDLE:
                return new MobileAppBundleId(uuid);
            case CALCULATED_FIELD:
                return new CalculatedFieldId(uuid);
            case CALCULATED_FIELD_LINK:
                return new CalculatedFieldLinkId(uuid);
        }
        throw new IllegalArgumentException("EntityType " + type + " is not supported!");
    }

}
