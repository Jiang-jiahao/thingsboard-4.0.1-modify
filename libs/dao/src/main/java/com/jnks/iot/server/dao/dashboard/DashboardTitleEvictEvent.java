package com.jnks.iot.server.dao.dashboard;

import lombok.Data;
import com.jnks.iot.server.common.data.id.DashboardId;

@Data
public class DashboardTitleEvictEvent {
    private final DashboardId key;
}
