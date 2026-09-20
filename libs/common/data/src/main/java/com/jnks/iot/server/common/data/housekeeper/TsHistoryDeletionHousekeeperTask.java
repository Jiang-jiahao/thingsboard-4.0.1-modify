package com.jnks.iot.server.common.data.housekeeper;

import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class TsHistoryDeletionHousekeeperTask extends HousekeeperTask {

    private String key;

    public TsHistoryDeletionHousekeeperTask(TenantId tenantId, EntityId entityId, String key) {
        super(tenantId, entityId, HousekeeperTaskType.DELETE_TS_HISTORY);
        this.key = key;
    }

    @Override
    public String getDescription() {
        return super.getDescription() + (key != null ? " for key '" + key + "'" : "");
    }

}
