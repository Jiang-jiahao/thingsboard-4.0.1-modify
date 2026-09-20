package com.jnks.iot.server.common.data.sync.vc;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.EntityType;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class EntityTypeLoadResult implements Serializable {
    private static final long serialVersionUID = -8428039809651395241L;

    private EntityType entityType;
    private int created;
    private int updated;
    private int deleted;

    public EntityTypeLoadResult(EntityType entityType) {
        this.entityType = entityType;
    }
}
