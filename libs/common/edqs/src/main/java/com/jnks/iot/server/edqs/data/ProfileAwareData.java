package com.jnks.iot.server.edqs.data;

import com.jnks.iot.server.common.data.edqs.fields.ProfileAwareFields;

import java.util.UUID;

public abstract class ProfileAwareData<T> extends BaseEntityData<ProfileAwareFields> {

    public ProfileAwareData(UUID id) {
        super(id);
    }

}
