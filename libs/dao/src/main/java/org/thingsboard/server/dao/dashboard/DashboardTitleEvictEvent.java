package org.thingsboard.server.dao.dashboard;

import lombok.Data;
import org.thingsboard.server.common.data.id.DashboardId;

@Data
public class DashboardTitleEvictEvent {
    private final DashboardId key;
}
