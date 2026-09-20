package com.jnks.iot.server.common.data.notification.info;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.TenantId;

import java.util.Map;

import static com.jnks.iot.server.common.data.util.CollectionsUtil.mapOf;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EntitiesLimitNotificationInfo implements RuleOriginatedNotificationInfo {

    private EntityType entityType;
    private long currentCount;
    private long limit;
    private int percents;
    private TenantId tenantId;
    private String tenantName;

    @Override
    public Map<String, String> getTemplateData() {
        return mapOf(
                "entityType", entityType.getNormalName(),
                "currentCount", String.valueOf(currentCount),
                "limit", String.valueOf(limit),
                "percents", String.valueOf(percents),
                "tenantId", tenantId.toString(),
                "tenantName", tenantName
        );
    }

    @Override
    public TenantId getAffectedTenantId() {
        return tenantId;
    }

}
