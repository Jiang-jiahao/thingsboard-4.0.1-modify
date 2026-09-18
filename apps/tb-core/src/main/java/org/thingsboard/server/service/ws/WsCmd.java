package org.thingsboard.server.service.ws;

import com.fasterxml.jackson.annotation.JsonIgnore;


public interface WsCmd {

    int getCmdId();

    @JsonIgnore
    WsCmdType getType();

}
