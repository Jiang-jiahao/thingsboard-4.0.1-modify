package org.thingsboard.server.cache;

import java.io.Serializable;

public interface VersionedCacheKey extends Serializable {

    default boolean isVersioned() {
        return false;
    }

}
