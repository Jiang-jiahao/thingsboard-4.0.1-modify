package com.jnks.iot.server.dao.model.sql;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.dao.model.ModelConstants;

@Data
@EqualsAndHashCode(callSuper = true)
@Entity
@Table(name = ModelConstants.TENANT_TABLE_NAME)
public final class TenantEntity extends AbstractTenantEntity<Tenant> {

    public TenantEntity() {
        super();
    }

    public TenantEntity(Tenant tenant) {
        super(tenant);
    }

    @Override
    public Tenant toData() {
        return super.toTenant();
    }
}
