package com.jnks.iot.server.controller.plugin;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class JnksIotWebSocketTextMsg implements JnksIotWebSocketMsg<String> {

    private final String value;

    @Override
    public JnksIotWebSocketMsgType getType() {
        return JnksIotWebSocketMsgType.TEXT;
    }

    @Override
    public String getMsg() {
        return value;
    }
}
