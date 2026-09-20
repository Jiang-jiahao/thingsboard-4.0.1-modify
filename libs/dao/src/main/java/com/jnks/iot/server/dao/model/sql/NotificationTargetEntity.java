package com.jnks.iot.server.dao.model.sql;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.id.NotificationTargetId;
import com.jnks.iot.server.common.data.notification.targets.NotificationTarget;
import com.jnks.iot.server.common.data.notification.targets.NotificationTargetConfig;
import com.jnks.iot.server.dao.model.BaseSqlEntity;
import com.jnks.iot.server.dao.model.ModelConstants;
import com.jnks.iot.server.dao.util.mapping.JsonConverter;

import java.util.UUID;

@Data
@EqualsAndHashCode(callSuper = true)
@Entity
@Table(name = ModelConstants.NOTIFICATION_TARGET_TABLE_NAME)
public class NotificationTargetEntity extends BaseSqlEntity<NotificationTarget> {

    @Column(name = ModelConstants.TENANT_ID_PROPERTY, nullable = false)
    private UUID tenantId;

    @Column(name = ModelConstants.NAME_PROPERTY, nullable = false)
    private String name;

    @Convert(converter = JsonConverter.class)
    @Column(name = ModelConstants.NOTIFICATION_TARGET_CONFIGURATION_PROPERTY, nullable = false)
    private JsonNode configuration;

    @Column(name = ModelConstants.EXTERNAL_ID_PROPERTY)
    private UUID externalId;

    public NotificationTargetEntity() {}

    public NotificationTargetEntity(NotificationTarget notificationTarget) {
        setId(notificationTarget.getUuidId());
        setCreatedTime(notificationTarget.getCreatedTime());
        setTenantId(getTenantUuid(notificationTarget.getTenantId()));
        setName(notificationTarget.getName());
        setConfiguration(toJson(notificationTarget.getConfiguration()));
        setExternalId(getUuid(notificationTarget.getExternalId()));
    }

    @Override
    public NotificationTarget toData() {
        NotificationTarget notificationTarget = new NotificationTarget();
        notificationTarget.setId(new NotificationTargetId(id));
        notificationTarget.setCreatedTime(createdTime);
        notificationTarget.setTenantId(getTenantId(tenantId));
        notificationTarget.setName(name);
        notificationTarget.setConfiguration(fromJson(configuration, NotificationTargetConfig.class));
        notificationTarget.setExternalId(getEntityId(externalId, NotificationTargetId::new));
        return notificationTarget;
    }

}
