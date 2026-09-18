package org.thingsboard.server.common.data;

public interface HasVersion {

    Long getVersion();

    default void setVersion(Long version) {
    }

}
