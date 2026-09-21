package com.jnks.iot.server.controller.plugin;

import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;

@RequiredArgsConstructor
public class JnksIotWebSocketPingMsg implements JnksIotWebSocketMsg<ByteBuffer> {

    public static JnksIotWebSocketPingMsg INSTANCE = new JnksIotWebSocketPingMsg();

    private static final ByteBuffer PING_MSG = ByteBuffer.wrap(new byte[]{});

    @Override
    public JnksIotWebSocketMsgType getType() {
        return JnksIotWebSocketMsgType.PING;
    }

    @Override
    public ByteBuffer getMsg() {
        return PING_MSG;
    }
}
