package org.thingsboard.server.common.data.housekeeper;

import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class LatestTsDeletionHousekeeperTask extends HousekeeperTask {

    private String key;

    public LatestTsDeletionHousekeeperTask(TenantId tenantId, EntityId entityId, String key) {
        super(tenantId, entityId, HousekeeperTaskType.DELETE_LATEST_TS);
        this.key = key;
    }

    @Override
    public String getDescription() {
        return super.getDescription() + (key != null ? " for key '" + key + "'" : "");
    }

}
