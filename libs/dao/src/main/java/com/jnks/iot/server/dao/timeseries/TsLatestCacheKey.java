package com.jnks.iot.server.dao.timeseries;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import com.jnks.iot.server.cache.VersionedCacheKey;
import com.jnks.iot.server.common.data.id.EntityId;

import java.io.Serial;

@EqualsAndHashCode
@Getter
@AllArgsConstructor
public class TsLatestCacheKey implements VersionedCacheKey {

    @Serial
    private static final long serialVersionUID = 2024369077925351881L;

    private final EntityId entityId;
    private final String key;

    @Override
    public String toString() {
        return "{" + entityId + "}" + key;
    }

    @Override
    public boolean isVersioned() {
        return true;
    }

}
