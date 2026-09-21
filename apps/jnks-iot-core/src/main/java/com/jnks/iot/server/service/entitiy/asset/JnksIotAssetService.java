package com.jnks.iot.server.service.entitiy.asset;

import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.asset.Asset;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.TenantId;

/**
 * 资产业务层契约：CRUD 以及分配到客户 / 公开客户。
 * <p>
 * 由 AssetController 调用；实现类委托 Asset DAO，并写审计日志。
 */
public interface JnksIotAssetService {

    /** 保存资产。 */
    Asset save(Asset asset, User user) throws Exception;

    /** 删除资产。 */
    void delete(Asset asset, User user);

    /** 将资产分配给客户。 */
    Asset assignAssetToCustomer(TenantId tenantId, AssetId assetId, Customer customer, User user) throws JnksIotException;

    /** 取消资产与客户的分配。 */
    Asset unassignAssetToCustomer(TenantId tenantId, AssetId assetId, Customer customer, User user) throws JnksIotException;

    /** 将资产分配给公开客户。 */
    Asset assignAssetToPublicCustomer(TenantId tenantId, AssetId assetId, User user) throws JnksIotException;

}
