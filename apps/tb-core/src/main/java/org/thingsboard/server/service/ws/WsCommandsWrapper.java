package org.thingsboard.server.service.ws;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.thingsboard.server.service.ws.notification.cmd.*;
import org.thingsboard.server.service.ws.telemetry.cmd.v1.AttributesSubscriptionCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v1.GetHistoryCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v1.TimeseriesSubscriptionCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.*;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WsCommandsWrapper {

    private AuthCmd authCmd;

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
            @Type(name = "ATTRIBUTES", value = AttributesSubscriptionCmd.class),
            @Type(name = "TIMESERIES", value = TimeseriesSubscriptionCmd.class),
            @Type(name = "TIMESERIES_HISTORY", value = GetHistoryCmd.class),
            @Type(name = "ENTITY_DATA", value = EntityDataCmd.class),
            @Type(name = "ENTITY_COUNT", value = EntityCountCmd.class),
            @Type(name = "ALARM_DATA", value = AlarmDataCmd.class),
            @Type(name = "ALARM_COUNT", value = AlarmCountCmd.class),
            @Type(name = "ALARM_STATUS", value = AlarmStatusCmd.class),
            @Type(name = "NOTIFICATIONS", value = NotificationsSubCmd.class),
            @Type(name = "NOTIFICATIONS_COUNT", value = NotificationsCountSubCmd.class),
            @Type(name = "MARK_NOTIFICATIONS_AS_READ", value = MarkNotificationsAsReadCmd.class),
            @Type(name = "MARK_ALL_NOTIFICATIONS_AS_READ", value = MarkAllNotificationsAsReadCmd.class),
            @Type(name = "ALARM_DATA_UNSUBSCRIBE", value = AlarmDataUnsubscribeCmd.class),
            @Type(name = "ALARM_COUNT_UNSUBSCRIBE", value = AlarmCountUnsubscribeCmd.class),
            @Type(name = "ENTITY_DATA_UNSUBSCRIBE", value = EntityDataUnsubscribeCmd.class),
            @Type(name = "ENTITY_COUNT_UNSUBSCRIBE", value = EntityCountUnsubscribeCmd.class),
            @Type(name = "NOTIFICATIONS_UNSUBSCRIBE", value = NotificationsUnsubCmd.class),
            @Type(name = "ALARM_STATUS_UNSUBSCRIBE", value = AlarmStatusUnsubscribeCmd.class),
    })
    private List<WsCmd> cmds;

}
