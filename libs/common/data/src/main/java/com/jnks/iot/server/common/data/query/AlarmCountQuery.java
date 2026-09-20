package com.jnks.iot.server.common.data.query;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import com.jnks.iot.server.common.data.alarm.AlarmSearchStatus;
import com.jnks.iot.server.common.data.alarm.AlarmSeverity;
import com.jnks.iot.server.common.data.id.UserId;

import java.util.List;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@ToString
public class AlarmCountQuery extends EntityCountQuery {
    private long startTs;
    private long endTs;
    private long timeWindow;
    private List<String> typeList;
    private List<AlarmSearchStatus> statusList;
    private List<AlarmSeverity> severityList;
    private boolean searchPropagatedAlarms;
    private UserId assigneeId;

    public AlarmCountQuery(EntityFilter entityFilter) {
        super(entityFilter);
    }

}
