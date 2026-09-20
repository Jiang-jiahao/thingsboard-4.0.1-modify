package com.jnks.iot.server.dao.model.sql;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.asset.Asset;

import static com.jnks.iot.server.dao.model.ModelConstants.ASSET_TABLE_NAME;

@Data
@EqualsAndHashCode(callSuper = true)
@Entity
@Table(name = ASSET_TABLE_NAME)
public final class AssetEntity extends AbstractAssetEntity<Asset> {

    public AssetEntity() {
        super();
    }

    public AssetEntity(Asset asset) {
        super(asset);
    }

    @Override
    public Asset toData() {
        return super.toAsset();
    }

}
