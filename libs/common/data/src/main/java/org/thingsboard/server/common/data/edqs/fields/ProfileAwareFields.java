package org.thingsboard.server.common.data.edqs.fields;

import java.util.UUID;

public interface ProfileAwareFields extends EntityFields {

    String getProfileName();

    UUID getProfileId();

}
