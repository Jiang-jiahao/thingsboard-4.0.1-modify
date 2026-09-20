package com.jnks.iot.server.common.data.id;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jnks.iot.server.common.data.EntityType;

import java.util.UUID;

public class MobileAppId extends UUIDBased implements EntityId{

    @JsonCreator
    public MobileAppId(@JsonProperty("id") UUID id) {
        super(id);
    }

    public static MobileAppId fromString(String mobileAppId) {
        return new MobileAppId(UUID.fromString(mobileAppId));
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.MOBILE_APP;
    }
}
