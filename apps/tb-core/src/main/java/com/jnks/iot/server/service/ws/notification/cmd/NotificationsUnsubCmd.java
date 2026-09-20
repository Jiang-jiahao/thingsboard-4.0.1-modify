package com.jnks.iot.server.service.ws.notification.cmd;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.service.ws.WsCmd;
import com.jnks.iot.server.service.ws.WsCmdType;
import com.jnks.iot.server.service.ws.telemetry.cmd.v2.UnsubscribeCmd;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NotificationsUnsubCmd implements UnsubscribeCmd, WsCmd {
    private int cmdId;

    @Override
    public WsCmdType getType() {
        return WsCmdType.NOTIFICATIONS_UNSUBSCRIBE;
    }
}
