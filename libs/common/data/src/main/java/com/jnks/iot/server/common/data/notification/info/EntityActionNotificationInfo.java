package com.jnks.iot.server.common.data.notification.info;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.notification.NotificationLabels;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.EntityId;

import java.util.Map;
import java.util.UUID;

import static com.jnks.iot.server.common.data.util.CollectionsUtil.mapOf;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EntityActionNotificationInfo implements RuleOriginatedNotificationInfo {

    private EntityId entityId;
    private String entityName;
    private ActionType actionType;
    private CustomerId entityCustomerId;

    private UUID userId;
    private String userTitle;
    private String userEmail;
    private String userFirstName;
    private String userLastName;

    @Override
    public Map<String, String> getTemplateData() {
        return mapOf(
                "entityType", entityId.getEntityType().getNormalName(),   // 链接里要用英文值
                "entityTypeLabel", NotificationLabels.entityType(entityId.getEntityType()),
                "entityId", entityId.toString(),
                "entityName", entityName,
                "actionType", NotificationLabels.actionType(actionType),
                "userId", userId.toString(),
                "userTitle", userTitle,
                "userEmail", userEmail,
                "userFirstName", userFirstName,
                "userLastName", userLastName
        );
    }

    @Override
    public CustomerId getAffectedCustomerId() {
        return entityCustomerId;
    }

    @Override
    public EntityId getStateEntityId() {
        return entityId;
    }

}
