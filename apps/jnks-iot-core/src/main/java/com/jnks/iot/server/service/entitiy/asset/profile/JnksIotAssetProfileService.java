package com.jnks.iot.server.service.entitiy.asset.profile;

import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.service.entitiy.SimpleJnksIotEntityService;

/**
 * 资产配置（Asset Profile）业务层契约。
 * <p>
 * 由 AssetProfileController 调用；实现类委托 DAO 并写审计日志。
 */
public interface JnksIotAssetProfileService extends SimpleJnksIotEntityService<AssetProfile> {

    /** 将指定配置设为租户默认资产配置。 */
    AssetProfile setDefaultAssetProfile(AssetProfile assetProfile, AssetProfile previousDefaultAssetProfile, User user) throws JnksIotException;
}
