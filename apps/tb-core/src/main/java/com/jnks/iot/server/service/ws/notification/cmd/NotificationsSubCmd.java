package com.jnks.iot.server.service.ws.notification.cmd;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.notification.NotificationType;
import com.jnks.iot.server.service.ws.WsCmd;
import com.jnks.iot.server.service.ws.WsCmdType;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NotificationsSubCmd implements WsCmd {
    private int cmdId;
    private int limit;
    private Set<NotificationType> types;

    @Override
    public WsCmdType getType() {
        return WsCmdType.NOTIFICATIONS;
    }
}
