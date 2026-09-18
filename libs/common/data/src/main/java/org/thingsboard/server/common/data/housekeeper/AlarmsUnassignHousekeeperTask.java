package org.thingsboard.server.common.data.housekeeper;

import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.UserId;

import java.util.List;
import java.util.UUID;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class AlarmsUnassignHousekeeperTask extends HousekeeperTask {

    private String userTitle;
    private List<UUID> alarms;

    protected AlarmsUnassignHousekeeperTask(User user) {
        this(user.getTenantId(), user.getId(), user.getTitle(), null);
    }

    public AlarmsUnassignHousekeeperTask(TenantId tenantId, UserId userId, String userTitle, List<UUID> alarms) {
        super(tenantId, userId, HousekeeperTaskType.UNASSIGN_ALARMS);
        this.userTitle = userTitle;
        this.alarms = alarms;
    }

    @Override
    public String getDescription() {
        return super.getDescription() + (alarms != null ? " (" + alarms + ")" : "");
    }

}
