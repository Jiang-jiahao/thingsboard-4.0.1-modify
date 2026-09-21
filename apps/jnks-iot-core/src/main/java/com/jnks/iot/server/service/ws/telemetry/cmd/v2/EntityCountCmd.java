package com.jnks.iot.server.service.ws.telemetry.cmd.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import com.jnks.iot.server.common.data.query.EntityCountQuery;
import com.jnks.iot.server.service.ws.WsCmdType;

public class EntityCountCmd extends DataCmd {

    @Getter
    private final EntityCountQuery query;

    @JsonCreator
    public EntityCountCmd(@JsonProperty("cmdId") int cmdId,
                          @JsonProperty("query") EntityCountQuery query) {
        super(cmdId);
        this.query = query;
    }

    @Override
    public WsCmdType getType() {
        return WsCmdType.ENTITY_COUNT;
    }
}
