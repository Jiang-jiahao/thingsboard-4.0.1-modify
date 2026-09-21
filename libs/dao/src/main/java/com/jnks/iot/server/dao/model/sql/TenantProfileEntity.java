package com.jnks.iot.server.dao.model.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.hibernate.annotations.JdbcType;
import org.hibernate.dialect.PostgreSQLJsonPGObjectJsonbType;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.id.TenantProfileId;
import com.jnks.iot.server.common.data.tenant.profile.TenantProfileData;
import com.jnks.iot.server.dao.model.BaseSqlEntity;
import com.jnks.iot.server.dao.model.ModelConstants;
import com.jnks.iot.server.dao.util.mapping.JsonConverter;

@Data
@EqualsAndHashCode(callSuper = true)
@Entity
@Table(name = ModelConstants.TENANT_PROFILE_TABLE_NAME)
public final class TenantProfileEntity extends BaseSqlEntity<TenantProfile> {

    @Column(name = ModelConstants.TENANT_PROFILE_NAME_PROPERTY)
    private String name;

    @Column(name = ModelConstants.TENANT_PROFILE_DESCRIPTION_PROPERTY)
    private String description;

    @Column(name = ModelConstants.TENANT_PROFILE_IS_DEFAULT_PROPERTY)
    private boolean isDefault;

    @Column(name = ModelConstants.TENANT_PROFILE_ISOLATED_TB_RULE_ENGINE)
    private boolean isolatedJnksIotRuleEngine;

    @Convert(converter = JsonConverter.class)
    @JdbcType(PostgreSQLJsonPGObjectJsonbType.class)
    @Column(name = ModelConstants.TENANT_PROFILE_PROFILE_DATA_PROPERTY)
    private JsonNode profileData;

    public TenantProfileEntity() {
        super();
    }

    public TenantProfileEntity(TenantProfile tenantProfile) {
        if (tenantProfile.getId() != null) {
            this.setUuid(tenantProfile.getId().getId());
        }
        this.setCreatedTime(tenantProfile.getCreatedTime());
        this.name = tenantProfile.getName();
        this.description = tenantProfile.getDescription();
        this.isDefault = tenantProfile.isDefault();
        this.isolatedJnksIotRuleEngine = tenantProfile.isIsolatedJnksIotRuleEngine();
        this.profileData = JacksonUtil.convertValue(tenantProfile.getProfileData(), ObjectNode.class);
    }

    @Override
    public TenantProfile toData() {
        TenantProfile tenantProfile = new TenantProfile(new TenantProfileId(this.getUuid()));
        tenantProfile.setCreatedTime(createdTime);
        tenantProfile.setName(name);
        tenantProfile.setDescription(description);
        tenantProfile.setDefault(isDefault);
        tenantProfile.setIsolatedJnksIotRuleEngine(isolatedJnksIotRuleEngine);
        tenantProfile.setProfileData(JacksonUtil.convertValue(profileData, TenantProfileData.class));
        return tenantProfile;
    }

}
