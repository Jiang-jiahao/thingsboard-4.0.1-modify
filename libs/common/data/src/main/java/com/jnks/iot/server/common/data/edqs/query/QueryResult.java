package com.jnks.iot.server.common.data.edqs.query;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.query.EntityData;
import com.jnks.iot.server.common.data.query.EntityKeyType;
import com.jnks.iot.server.common.data.query.TsValue;

import java.util.Collections;
import java.util.Map;

@Data
@RequiredArgsConstructor
public class QueryResult {

    private final EntityId entityId;
    private final Map<EntityKeyType, Map<String, TsValue>> latest;

    public EntityData toOldEntityData() {
        return new EntityData(entityId, latest, Collections.emptyMap(), Collections.emptyMap());
    }

}
