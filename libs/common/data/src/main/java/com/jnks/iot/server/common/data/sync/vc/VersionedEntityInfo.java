package com.jnks.iot.server.common.data.sync.vc;

import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.id.EntityId;

@Data
@NoArgsConstructor
public class VersionedEntityInfo {
    private EntityId externalId;

    public VersionedEntityInfo(EntityId externalId) {
        this.externalId = externalId;
    }
}
