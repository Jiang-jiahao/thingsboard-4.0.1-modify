package com.jnks.iot.server.edqs.data;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.relation.RelationTypeGroup;

import java.util.UUID;

public record RelationData(UUID fromId, EntityType fromType, UUID toId, EntityType toType, String type,
                           RelationTypeGroup typeGroup) {

}
