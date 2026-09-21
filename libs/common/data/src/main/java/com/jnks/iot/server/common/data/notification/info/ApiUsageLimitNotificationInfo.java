package com.jnks.iot.server.common.data.notification.info;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.notification.NotificationLabels;
import com.jnks.iot.server.common.data.ApiFeature;
import com.jnks.iot.server.common.data.ApiUsageRecordKey;
import com.jnks.iot.server.common.data.ApiUsageStateValue;
import com.jnks.iot.server.common.data.id.TenantId;

import java.util.Map;

import static com.jnks.iot.server.common.data.util.CollectionsUtil.mapOf;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ApiUsageLimitNotificationInfo implements RuleOriginatedNotificationInfo {

    private ApiFeature feature;
    private ApiUsageRecordKey recordKey;
    private ApiUsageStateValue status;
    private String limit;
    private String currentValue;
    private TenantId tenantId;
    private String tenantName;

    @Override
    public Map<String, String> getTemplateData() {
        return mapOf(
                "feature", NotificationLabels.apiFeature(feature),
                "unitLabel", recordKey.getUnitLabel(),
                "status", NotificationLabels.apiUsageStatus(status.name().toLowerCase()),
                "limit", limit,
                "currentValue", currentValue,
                "tenantId", tenantId.toString(),
                "tenantName", tenantName
        );
    }

    @Override
    public TenantId getAffectedTenantId() {
        return tenantId;
    }

}
