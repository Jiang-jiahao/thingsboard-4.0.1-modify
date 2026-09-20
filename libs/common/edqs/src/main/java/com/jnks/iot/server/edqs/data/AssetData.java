package com.jnks.iot.server.edqs.data;

import lombok.ToString;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.edqs.fields.AssetFields;

import java.util.UUID;

@ToString(callSuper = true)
public class AssetData extends ProfileAwareData<AssetFields> {

    public AssetData(UUID id) {
        super(id);
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.ASSET;
    }

}
