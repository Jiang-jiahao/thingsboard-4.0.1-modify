package org.thingsboard.server.service.entitiy.asset.profile;

import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.asset.AssetProfile;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.service.entitiy.SimpleTbEntityService;

/**
 * 资产配置（Asset Profile）业务层契约。
 * <p>
 * 由 AssetProfileController 调用；实现类委托 DAO 并写审计日志。
 */
public interface TbAssetProfileService extends SimpleTbEntityService<AssetProfile> {

    /** 将指定配置设为租户默认资产配置。 */
    AssetProfile setDefaultAssetProfile(AssetProfile assetProfile, AssetProfile previousDefaultAssetProfile, User user) throws ThingsboardException;
}
