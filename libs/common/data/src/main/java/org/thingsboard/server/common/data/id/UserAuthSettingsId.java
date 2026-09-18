package org.thingsboard.server.common.data.id;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public class UserAuthSettingsId extends UUIDBased {

    @JsonCreator
    public UserAuthSettingsId(@JsonProperty("id") UUID id) {
        super(id);
    }

}
