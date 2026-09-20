package com.jnks.iot.server.common.data.alarm;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.page.TimePageLink;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
public class AlarmQueryV2 {

    private EntityId affectedEntityId;
    private TimePageLink pageLink;
    private List<String> typeList;
    private List<AlarmSearchStatus> statusList;
    private List<AlarmSeverity> severityList;
    private UserId assigneeId;

}
