package com.jnks.iot.server.controller.plugin;

public interface JnksIotWebSocketMsg<T> {

    JnksIotWebSocketMsgType getType();

    T getMsg();

}
