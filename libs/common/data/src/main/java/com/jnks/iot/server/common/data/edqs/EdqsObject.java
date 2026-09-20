package com.jnks.iot.server.common.data.edqs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.jnks.iot.server.common.data.ObjectType;

public interface EdqsObject {

    @JsonIgnore
    String key();

    @JsonIgnore
    Long version();

    @JsonIgnore
    ObjectType type();

}
