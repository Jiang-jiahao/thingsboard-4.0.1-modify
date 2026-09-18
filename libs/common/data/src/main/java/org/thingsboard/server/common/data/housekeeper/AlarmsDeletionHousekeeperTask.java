package org.thingsboard.server.common.data.housekeeper;

import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;

import java.util.List;
import java.util.UUID;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class AlarmsDeletionHousekeeperTask extends HousekeeperTask {

    private List<UUID> alarms;

    public AlarmsDeletionHousekeeperTask(TenantId tenantId, EntityId entityId) {
        this(tenantId, entityId, null);
    }

    public AlarmsDeletionHousekeeperTask(TenantId tenantId, EntityId entityId, List<UUID> alarms) {
        super(tenantId, entityId, HousekeeperTaskType.DELETE_ALARMS);
        this.alarms = alarms;
    }

    @Override
    public String getDescription() {
        return super.getDescription() + (alarms != null ? " (" + alarms + ")" : "");
    }

}
