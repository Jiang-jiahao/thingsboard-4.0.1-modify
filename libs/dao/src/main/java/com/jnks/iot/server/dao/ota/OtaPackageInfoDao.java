package com.jnks.iot.server.dao.ota;

import com.jnks.iot.server.common.data.OtaPackageInfo;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.OtaPackageId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.ota.OtaPackageType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;

public interface OtaPackageInfoDao extends Dao<OtaPackageInfo> {

    PageData<OtaPackageInfo> findOtaPackageInfoByTenantId(TenantId tenantId, PageLink pageLink);

    PageData<OtaPackageInfo> findOtaPackageInfoByTenantIdAndDeviceProfileIdAndTypeAndHasData(TenantId tenantId, DeviceProfileId deviceProfileId, OtaPackageType otaPackageType, PageLink pageLink);

    boolean isOtaPackageUsed(OtaPackageId otaPackageId, OtaPackageType otaPackageType, DeviceProfileId deviceProfileId);

}
