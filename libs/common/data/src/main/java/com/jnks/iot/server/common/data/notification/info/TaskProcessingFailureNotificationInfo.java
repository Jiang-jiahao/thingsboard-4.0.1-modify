package com.jnks.iot.server.common.data.notification.info;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.housekeeper.HousekeeperTaskType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;

import java.util.Map;

import static com.jnks.iot.server.common.data.util.CollectionsUtil.mapOf;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TaskProcessingFailureNotificationInfo implements RuleOriginatedNotificationInfo {

    private TenantId tenantId;
    private EntityId entityId;
    private HousekeeperTaskType taskType;
    private String taskDescription;
    private String error;
    private int attempt;

    @Override
    public Map<String, String> getTemplateData() {
        return mapOf(
                "tenantId", tenantId.toString(),
                "entityType", entityId.getEntityType().getNormalName(),
                "entityId", entityId.getId().toString(),
                "taskType", taskType.getDescription(),
                "taskDescription", taskDescription,
                "error", error,
                "attempt", String.valueOf(attempt)
        );
    }

    @Override
    public TenantId getAffectedTenantId() {
        return tenantId;
    }

}
