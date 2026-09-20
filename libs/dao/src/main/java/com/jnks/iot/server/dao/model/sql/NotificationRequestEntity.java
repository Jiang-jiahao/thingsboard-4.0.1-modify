package com.jnks.iot.server.dao.model.sql;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.id.NotificationRequestId;
import com.jnks.iot.server.common.data.id.NotificationRuleId;
import com.jnks.iot.server.common.data.id.NotificationTemplateId;
import com.jnks.iot.server.common.data.notification.NotificationRequest;
import com.jnks.iot.server.common.data.notification.NotificationRequestConfig;
import com.jnks.iot.server.common.data.notification.NotificationRequestStats;
import com.jnks.iot.server.common.data.notification.NotificationRequestStatus;
import com.jnks.iot.server.common.data.notification.info.NotificationInfo;
import com.jnks.iot.server.common.data.notification.template.NotificationTemplate;
import com.jnks.iot.server.dao.model.BaseSqlEntity;
import com.jnks.iot.server.dao.model.ModelConstants;
import com.jnks.iot.server.dao.util.mapping.JsonConverter;

import java.util.UUID;

@Data
@EqualsAndHashCode(callSuper = true)
@Entity
@Table(name = ModelConstants.NOTIFICATION_REQUEST_TABLE_NAME)
public class NotificationRequestEntity extends BaseSqlEntity<NotificationRequest> {

    @Column(name = ModelConstants.TENANT_ID_PROPERTY, nullable = false)
    private UUID tenantId;

    @Column(name = ModelConstants.NOTIFICATION_REQUEST_TARGETS_PROPERTY, nullable = false)
    private String targets;

    @Column(name = ModelConstants.NOTIFICATION_REQUEST_TEMPLATE_ID_PROPERTY)
    private UUID templateId;

    @Convert(converter = JsonConverter.class)
    @Column(name = ModelConstants.NOTIFICATION_REQUEST_TEMPLATE_PROPERTY)
    private JsonNode template;

    @Convert(converter = JsonConverter.class)
    @Column(name = ModelConstants.NOTIFICATION_REQUEST_INFO_PROPERTY)
    private JsonNode info;

    @Convert(converter = JsonConverter.class)
    @Column(name = ModelConstants.NOTIFICATION_REQUEST_ADDITIONAL_CONFIG_PROPERTY)
    private JsonNode additionalConfig;

    @Column(name = ModelConstants.NOTIFICATION_REQUEST_ORIGINATOR_ENTITY_ID_PROPERTY)
    private UUID originatorEntityId;

    @Enumerated(EnumType.STRING)
    @Column(name = ModelConstants.NOTIFICATION_REQUEST_ORIGINATOR_ENTITY_TYPE_PROPERTY)
    private EntityType originatorEntityType;

    @Column(name = ModelConstants.NOTIFICATION_REQUEST_RULE_ID_PROPERTY)
    private UUID ruleId;

    @Enumerated(EnumType.STRING)
    @Column(name = ModelConstants.NOTIFICATION_REQUEST_STATUS_PROPERTY)
    private NotificationRequestStatus status;

    @Convert(converter = JsonConverter.class)
    @Column(name = ModelConstants.NOTIFICATION_REQUEST_STATS_PROPERTY)
    private JsonNode stats;

    public NotificationRequestEntity() {}

    public NotificationRequestEntity(NotificationRequest notificationRequest) {
        setId(notificationRequest.getUuidId());
        setCreatedTime(notificationRequest.getCreatedTime());
        setTenantId(getTenantUuid(notificationRequest.getTenantId()));
        setTargets(listToString(notificationRequest.getTargets()));
        setTemplateId(getUuid(notificationRequest.getTemplateId()));
        setTemplate(toJson(notificationRequest.getTemplate()));
        setInfo(toJson(notificationRequest.getInfo()));
        setAdditionalConfig(toJson(notificationRequest.getAdditionalConfig()));
        if (notificationRequest.getOriginatorEntityId() != null) {
            setOriginatorEntityId(notificationRequest.getOriginatorEntityId().getId());
            setOriginatorEntityType(notificationRequest.getOriginatorEntityId().getEntityType());
        }
        setRuleId(getUuid(notificationRequest.getRuleId()));
        setStatus(notificationRequest.getStatus());
        setStats(toJson(notificationRequest.getStats()));
    }

    public NotificationRequestEntity(NotificationRequestEntity other) {
        this.id = other.id;
        this.createdTime = other.createdTime;
        this.tenantId = other.tenantId;
        this.targets = other.targets;
        this.templateId = other.templateId;
        this.template = other.template;
        this.info = other.info;
        this.additionalConfig = other.additionalConfig;
        this.originatorEntityId = other.originatorEntityId;
        this.originatorEntityType = other.originatorEntityType;
        this.ruleId = other.ruleId;
        this.status = other.status;
        this.stats = other.stats;
    }

    @Override
    public NotificationRequest toData() {
        NotificationRequest notificationRequest = new NotificationRequest();
        notificationRequest.setId(new NotificationRequestId(id));
        notificationRequest.setCreatedTime(createdTime);
        notificationRequest.setTenantId(getTenantId(tenantId));
        notificationRequest.setTargets(listFromString(targets, UUID::fromString));
        notificationRequest.setTemplateId(getEntityId(templateId, NotificationTemplateId::new));
        notificationRequest.setTemplate(fromJson(template, NotificationTemplate.class));
        notificationRequest.setInfo(fromJson(info, NotificationInfo.class));
        notificationRequest.setAdditionalConfig(fromJson(additionalConfig, NotificationRequestConfig.class));
        if (originatorEntityId != null) {
            notificationRequest.setOriginatorEntityId(EntityIdFactory.getByTypeAndUuid(originatorEntityType, originatorEntityId));
        }
        notificationRequest.setRuleId(getEntityId(ruleId, NotificationRuleId::new));
        notificationRequest.setStatus(status);
        notificationRequest.setStats(fromJson(stats, NotificationRequestStats.class));
        return notificationRequest;
    }

}
