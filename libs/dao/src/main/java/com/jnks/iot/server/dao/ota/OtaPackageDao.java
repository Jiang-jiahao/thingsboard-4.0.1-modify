package com.jnks.iot.server.dao.ota;

import com.jnks.iot.server.common.data.OtaPackage;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.Dao;
import com.jnks.iot.server.dao.TenantEntityWithDataDao;

public interface OtaPackageDao extends Dao<OtaPackage>, TenantEntityWithDataDao {

    Long sumDataSizeByTenantId(TenantId tenantId);

}
