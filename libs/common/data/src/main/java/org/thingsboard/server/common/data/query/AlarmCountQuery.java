package org.thingsboard.server.common.data.query;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.thingsboard.server.common.data.alarm.AlarmSearchStatus;
import org.thingsboard.server.common.data.alarm.AlarmSeverity;
import org.thingsboard.server.common.data.id.UserId;

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
