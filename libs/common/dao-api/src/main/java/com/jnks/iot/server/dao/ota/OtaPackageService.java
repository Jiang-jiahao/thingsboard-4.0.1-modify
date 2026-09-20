package com.jnks.iot.server.dao.ota;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.OtaPackage;
import com.jnks.iot.server.common.data.OtaPackageInfo;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.OtaPackageId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.ota.ChecksumAlgorithm;
import com.jnks.iot.server.common.data.ota.OtaPackageType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.entity.EntityDaoService;

import java.nio.ByteBuffer;

public interface OtaPackageService extends EntityDaoService {

    OtaPackageInfo saveOtaPackageInfo(OtaPackageInfo otaPackageInfo, boolean isUrl);

    OtaPackage saveOtaPackage(OtaPackage otaPackage);

    String generateChecksum(ChecksumAlgorithm checksumAlgorithm, ByteBuffer data);

    OtaPackage findOtaPackageById(TenantId tenantId, OtaPackageId otaPackageId);

    OtaPackageInfo findOtaPackageInfoById(TenantId tenantId, OtaPackageId otaPackageId);

    ListenableFuture<OtaPackageInfo> findOtaPackageInfoByIdAsync(TenantId tenantId, OtaPackageId otaPackageId);

    PageData<OtaPackageInfo> findTenantOtaPackagesByTenantId(TenantId tenantId, PageLink pageLink);

    PageData<OtaPackageInfo> findTenantOtaPackagesByTenantIdAndDeviceProfileIdAndTypeAndHasData(TenantId tenantId, DeviceProfileId deviceProfileId, OtaPackageType otaPackageType, PageLink pageLink);

    void deleteOtaPackage(TenantId tenantId, OtaPackageId otaPackageId);

    void deleteOtaPackagesByTenantId(TenantId tenantId);

    long sumDataSizeByTenantId(TenantId tenantId);
}
