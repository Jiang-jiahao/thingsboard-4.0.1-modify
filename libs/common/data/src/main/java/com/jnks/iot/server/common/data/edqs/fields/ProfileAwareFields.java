package com.jnks.iot.server.common.data.edqs.fields;

import java.util.UUID;

public interface ProfileAwareFields extends EntityFields {

    String getProfileName();

    UUID getProfileId();

}
