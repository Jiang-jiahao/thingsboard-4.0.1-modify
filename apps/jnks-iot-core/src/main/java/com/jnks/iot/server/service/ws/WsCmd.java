package com.jnks.iot.server.service.ws;

import com.fasterxml.jackson.annotation.JsonIgnore;


public interface WsCmd {

    int getCmdId();

    @JsonIgnore
    WsCmdType getType();

}
