package com.jnks.iot.server.service.ws.notification;

import com.jnks.iot.server.service.ws.WebSocketSessionRef;
import com.jnks.iot.server.service.ws.notification.cmd.MarkAllNotificationsAsReadCmd;
import com.jnks.iot.server.service.ws.notification.cmd.MarkNotificationsAsReadCmd;
import com.jnks.iot.server.service.ws.notification.cmd.NotificationsCountSubCmd;
import com.jnks.iot.server.service.ws.notification.cmd.NotificationsSubCmd;
import com.jnks.iot.server.service.ws.telemetry.cmd.v2.UnsubscribeCmd;

public interface NotificationCommandsHandler {

    void handleUnreadNotificationsSubCmd(WebSocketSessionRef sessionRef, NotificationsSubCmd cmd);

    void handleUnreadNotificationsCountSubCmd(WebSocketSessionRef sessionRef, NotificationsCountSubCmd cmd);

    void handleMarkAsReadCmd(WebSocketSessionRef sessionRef, MarkNotificationsAsReadCmd cmd);

    void handleMarkAllAsReadCmd(WebSocketSessionRef sessionRef, MarkAllNotificationsAsReadCmd cmd);

    void handleUnsubCmd(WebSocketSessionRef sessionRef, UnsubscribeCmd cmd);

}
