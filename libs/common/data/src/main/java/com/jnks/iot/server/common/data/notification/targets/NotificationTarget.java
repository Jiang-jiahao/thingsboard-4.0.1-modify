package com.jnks.iot.server.common.data.notification.targets;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.BaseData;
import com.jnks.iot.server.common.data.ExportableEntity;
import com.jnks.iot.server.common.data.HasName;
import com.jnks.iot.server.common.data.HasTenantId;
import com.jnks.iot.server.common.data.id.NotificationTargetId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.validation.Length;
import com.jnks.iot.server.common.data.validation.NoXss;

@Data
@EqualsAndHashCode(callSuper = true)
public class NotificationTarget extends BaseData<NotificationTargetId> implements HasTenantId, HasName, ExportableEntity<NotificationTargetId> {

    private TenantId tenantId;
    @NotBlank
    @NoXss
    @Length(max = 255, message = "cannot be longer than 255 chars")
    private String name;
    @NotNull
    @Valid
    private NotificationTargetConfig configuration;

    private NotificationTargetId externalId;

    public NotificationTarget() {
    }

    public NotificationTarget(NotificationTarget other) {
        super(other);
        this.tenantId = other.tenantId;
        this.name = other.name;
        this.configuration = other.configuration;
        this.externalId = other.externalId;
    }

}
