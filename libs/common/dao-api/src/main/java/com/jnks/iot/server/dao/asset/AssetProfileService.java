package com.jnks.iot.server.dao.asset;

import com.jnks.iot.server.common.data.EntityInfo;
import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.asset.AssetProfileInfo;
import com.jnks.iot.server.common.data.id.AssetProfileId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.entity.EntityDaoService;

import java.util.List;

public interface AssetProfileService extends EntityDaoService {

    AssetProfile findAssetProfileById(TenantId tenantId, AssetProfileId assetProfileId);

    AssetProfile findAssetProfileById(TenantId tenantId, AssetProfileId assetProfileId, boolean putInCache);

    AssetProfile findAssetProfileByName(TenantId tenantId, String profileName);

    AssetProfile findAssetProfileByName(TenantId tenantId, String profileName, boolean putInCache);

    AssetProfileInfo findAssetProfileInfoById(TenantId tenantId, AssetProfileId assetProfileId);

    AssetProfile saveAssetProfile(AssetProfile assetProfile);

    AssetProfile saveAssetProfile(AssetProfile assetProfile, boolean doValidate, boolean publishSaveEvent);

    void deleteAssetProfile(TenantId tenantId, AssetProfileId assetProfileId);

    PageData<AssetProfile> findAssetProfiles(TenantId tenantId, PageLink pageLink);

    PageData<AssetProfileInfo> findAssetProfileInfos(TenantId tenantId, PageLink pageLink);

    AssetProfile findOrCreateAssetProfile(TenantId tenantId, String profileName);

    AssetProfile createDefaultAssetProfile(TenantId tenantId);

    AssetProfile findDefaultAssetProfile(TenantId tenantId);

    AssetProfileInfo findDefaultAssetProfileInfo(TenantId tenantId);

    boolean setDefaultAssetProfile(TenantId tenantId, AssetProfileId assetProfileId);

    void deleteAssetProfilesByTenantId(TenantId tenantId);

    List<EntityInfo> findAssetProfileNamesByTenantId(TenantId tenantId, boolean activeOnly);

}
