package com.jnks.iot.server.dao.model.sql;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.cf.CalculatedField;
import com.jnks.iot.server.common.data.cf.CalculatedFieldType;
import com.jnks.iot.server.common.data.cf.configuration.CalculatedFieldConfiguration;
import com.jnks.iot.server.common.data.debug.DebugSettings;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.model.BaseEntity;
import com.jnks.iot.server.dao.model.BaseVersionedEntity;
import com.jnks.iot.server.dao.util.mapping.JsonConverter;

import java.util.UUID;

import static com.jnks.iot.server.dao.model.ModelConstants.CALCULATED_FIELD_CONFIGURATION;
import static com.jnks.iot.server.dao.model.ModelConstants.CALCULATED_FIELD_CONFIGURATION_VERSION;
import static com.jnks.iot.server.dao.model.ModelConstants.CALCULATED_FIELD_ENTITY_ID;
import static com.jnks.iot.server.dao.model.ModelConstants.CALCULATED_FIELD_ENTITY_TYPE;
import static com.jnks.iot.server.dao.model.ModelConstants.CALCULATED_FIELD_NAME;
import static com.jnks.iot.server.dao.model.ModelConstants.CALCULATED_FIELD_TABLE_NAME;
import static com.jnks.iot.server.dao.model.ModelConstants.CALCULATED_FIELD_TENANT_ID_COLUMN;
import static com.jnks.iot.server.dao.model.ModelConstants.CALCULATED_FIELD_TYPE;
import static com.jnks.iot.server.dao.model.ModelConstants.CALCULATED_FIELD_VERSION;
import static com.jnks.iot.server.dao.model.ModelConstants.DEBUG_SETTINGS;

@Data
@EqualsAndHashCode(callSuper = true)
@Entity
@Table(name = CALCULATED_FIELD_TABLE_NAME)
public class CalculatedFieldEntity extends BaseVersionedEntity<CalculatedField> implements BaseEntity<CalculatedField> {

    @Column(name = CALCULATED_FIELD_TENANT_ID_COLUMN)
    private UUID tenantId;

    @Column(name = CALCULATED_FIELD_ENTITY_TYPE)
    private String entityType;

    @Column(name = CALCULATED_FIELD_ENTITY_ID)
    private UUID entityId;

    @Column(name = CALCULATED_FIELD_TYPE)
    private String type;

    @Column(name = CALCULATED_FIELD_NAME)
    private String name;

    @Column(name = CALCULATED_FIELD_CONFIGURATION_VERSION)
    private int configurationVersion;

    @Convert(converter = JsonConverter.class)
    @Column(name = CALCULATED_FIELD_CONFIGURATION)
    private JsonNode configuration;

    @Column(name = CALCULATED_FIELD_VERSION)
    private Long version;

    @Column(name = DEBUG_SETTINGS)
    private String debugSettings;

    public CalculatedFieldEntity() {
        super();
    }

    public CalculatedFieldEntity(CalculatedField calculatedField) {
        this.setUuid(calculatedField.getUuidId());
        this.createdTime = calculatedField.getCreatedTime();
        this.tenantId = calculatedField.getTenantId().getId();
        this.entityType = calculatedField.getEntityId().getEntityType().name();
        this.entityId = calculatedField.getEntityId().getId();
        this.type = calculatedField.getType().name();
        this.name = calculatedField.getName();
        this.configurationVersion = calculatedField.getConfigurationVersion();
        this.configuration = JacksonUtil.valueToTree(calculatedField.getConfiguration());
        this.version = calculatedField.getVersion();
        this.debugSettings = JacksonUtil.toString(calculatedField.getDebugSettings());
    }

    @Override
    public CalculatedField toData() {
        CalculatedField calculatedField = new CalculatedField(new CalculatedFieldId(id));
        calculatedField.setCreatedTime(createdTime);
        calculatedField.setTenantId(TenantId.fromUUID(tenantId));
        calculatedField.setEntityId(EntityIdFactory.getByTypeAndUuid(entityType, entityId));
        calculatedField.setType(CalculatedFieldType.valueOf(type));
        calculatedField.setName(name);
        calculatedField.setConfigurationVersion(configurationVersion);
        calculatedField.setConfiguration(JacksonUtil.treeToValue(configuration, CalculatedFieldConfiguration.class));
        calculatedField.setVersion(version);
        calculatedField.setDebugSettings(JacksonUtil.fromString(debugSettings, DebugSettings.class));
        return calculatedField;
    }

}
