package com.jnks.iot.server.dao.asset;

import com.jnks.iot.server.common.data.EntityInfo;
import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.asset.AssetProfileInfo;
import com.jnks.iot.server.common.data.id.AssetProfileId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;
import com.jnks.iot.server.dao.ExportableEntityDao;
import com.jnks.iot.server.dao.ImageContainerDao;

import java.util.List;
import java.util.UUID;

public interface AssetProfileDao extends Dao<AssetProfile>, ExportableEntityDao<AssetProfileId, AssetProfile>, ImageContainerDao<AssetProfileInfo> {

    AssetProfileInfo findAssetProfileInfoById(TenantId tenantId, UUID assetProfileId);

    AssetProfile save(TenantId tenantId, AssetProfile assetProfile);

    AssetProfile saveAndFlush(TenantId tenantId, AssetProfile assetProfile);

    PageData<AssetProfile> findAssetProfiles(TenantId tenantId, PageLink pageLink);

    PageData<AssetProfileInfo> findAssetProfileInfos(TenantId tenantId, PageLink pageLink);

    AssetProfile findDefaultAssetProfile(TenantId tenantId);

    AssetProfileInfo findDefaultAssetProfileInfo(TenantId tenantId);

    AssetProfile findByName(TenantId tenantId, String profileName);

    PageData<AssetProfile> findAllWithImages(PageLink pageLink);

    List<EntityInfo> findTenantAssetProfileNames(UUID tenantId, boolean activeOnly);

}
