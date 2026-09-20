package com.jnks.iot.server.edqs.data;

import lombok.Data;

@Data
public class RelationInfo {

    private final String type;
    private final EntityData<?> target;

}
