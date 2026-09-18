package org.thingsboard.monitoring.data.cmd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.thingsboard.server.common.data.query.EntityData;
import org.thingsboard.server.common.data.query.EntityKeyType;
import org.thingsboard.server.common.data.query.TsValue;

import java.util.List;
import java.util.UUID;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class EntityDataUpdate {

    @JsonIgnoreProperties(ignoreUnknown = true)
    private List<EntityData> update;

    public String getLatest(UUID entityId, String key) {
        if (update == null) return null;

        return update.stream()
                .filter(entityData -> entityData.getEntityId().getId().equals(entityId)).findFirst()
                .map(EntityData::getLatest).map(latest -> latest.get(EntityKeyType.TIME_SERIES))
                .map(latest -> latest.get(key)).map(TsValue::getValue)
                .orElse(null);
    }

}
