package com.jnks.iot.server.controller.plugin;

public interface TbWebSocketMsg<T> {

    TbWebSocketMsgType getType();

    T getMsg();

}
