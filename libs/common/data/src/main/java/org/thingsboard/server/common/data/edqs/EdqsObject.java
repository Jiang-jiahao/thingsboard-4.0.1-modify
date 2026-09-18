package org.thingsboard.server.common.data.edqs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.thingsboard.server.common.data.ObjectType;

public interface EdqsObject {

    @JsonIgnore
    String key();

    @JsonIgnore
    Long version();

    @JsonIgnore
    ObjectType type();

}
