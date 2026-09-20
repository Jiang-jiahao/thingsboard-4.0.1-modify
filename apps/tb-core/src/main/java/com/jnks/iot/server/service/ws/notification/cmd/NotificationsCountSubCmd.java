package com.jnks.iot.server.service.ws.notification.cmd;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.service.ws.WsCmd;
import com.jnks.iot.server.service.ws.WsCmdType;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NotificationsCountSubCmd implements WsCmd {
    private int cmdId;

    @Override
    public WsCmdType getType() {
        return WsCmdType.NOTIFICATIONS_COUNT;
    }
}
