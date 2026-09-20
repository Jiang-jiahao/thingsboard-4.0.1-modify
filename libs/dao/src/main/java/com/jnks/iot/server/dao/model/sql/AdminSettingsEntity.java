package com.jnks.iot.server.dao.model.sql;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.AdminSettings;
import com.jnks.iot.server.common.data.id.AdminSettingsId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.model.BaseEntity;
import com.jnks.iot.server.dao.model.BaseSqlEntity;
import com.jnks.iot.server.dao.model.ModelConstants;
import com.jnks.iot.server.dao.util.mapping.JsonConverter;

import java.util.UUID;

import static com.jnks.iot.server.dao.model.ModelConstants.ADMIN_SETTINGS_JSON_VALUE_PROPERTY;
import static com.jnks.iot.server.dao.model.ModelConstants.ADMIN_SETTINGS_KEY_PROPERTY;
import static com.jnks.iot.server.dao.model.ModelConstants.ADMIN_SETTINGS_TABLE_NAME;

@Data
@EqualsAndHashCode(callSuper = true)
@Entity
@Table(name = ADMIN_SETTINGS_TABLE_NAME)
public final class AdminSettingsEntity extends BaseSqlEntity<AdminSettings> implements BaseEntity<AdminSettings> {

    @Column(name = ModelConstants.ADMIN_SETTINGS_TENANT_ID_PROPERTY)
    private UUID tenantId;

    @Column(name = ADMIN_SETTINGS_KEY_PROPERTY)
    private String key;

    @Convert(converter = JsonConverter.class)
    @Column(name = ADMIN_SETTINGS_JSON_VALUE_PROPERTY)
    private JsonNode jsonValue;

    public AdminSettingsEntity() {
        super();
    }

    public AdminSettingsEntity(AdminSettings adminSettings) {
        if (adminSettings.getId() != null) {
            this.setUuid(adminSettings.getId().getId());
        }
        this.setCreatedTime(adminSettings.getCreatedTime());
        this.tenantId = adminSettings.getTenantId().getId();
        this.key = adminSettings.getKey();
        this.jsonValue = adminSettings.getJsonValue();
    }

    @Override
    public AdminSettings toData() {
        AdminSettings adminSettings = new AdminSettings(new AdminSettingsId(id));
        adminSettings.setCreatedTime(createdTime);
        adminSettings.setTenantId(TenantId.fromUUID(tenantId));
        adminSettings.setKey(key);
        adminSettings.setJsonValue(jsonValue);
        return adminSettings;
    }

}
