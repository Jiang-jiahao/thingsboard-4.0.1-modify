package com.jnks.iot.server.common.data.alarm;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.page.TimePageLink;

/**
 * Created by ashvayka on 11.05.17.
 */
@Data
@Builder
@AllArgsConstructor
public class AlarmQuery {

    private EntityId affectedEntityId;
    private TimePageLink pageLink;
    private AlarmSearchStatus searchStatus;
    private AlarmStatus status;
    private UserId assigneeId;
    @Deprecated
    private Boolean fetchOriginator;

}
