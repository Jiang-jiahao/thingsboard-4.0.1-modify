package com.jnks.iot.server.dao.attributes;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import com.jnks.iot.server.cache.VersionedCacheKey;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.id.EntityId;

import java.io.Serial;

@EqualsAndHashCode
@Getter
@AllArgsConstructor
public class AttributeCacheKey implements VersionedCacheKey {

    @Serial
    private static final long serialVersionUID = 2013369077925351881L;

    private final AttributeScope scope;
    private final EntityId entityId;
    private final String key;

    @Override
    public String toString() {
        return "{" + entityId + "}" + scope + "_" + key;
    }

    @Override
    public boolean isVersioned() {
        return true;
    }

}
