package com.jnks.iot.server.dao.device;

import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.DeviceProfileInfo;
import com.jnks.iot.server.common.data.DeviceTransportType;
import com.jnks.iot.server.common.data.EntityInfo;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;
import com.jnks.iot.server.dao.ExportableEntityDao;
import com.jnks.iot.server.dao.ImageContainerDao;

import java.util.List;
import java.util.UUID;

public interface DeviceProfileDao extends Dao<DeviceProfile>, ExportableEntityDao<DeviceProfileId, DeviceProfile>, ImageContainerDao<DeviceProfileInfo> {

    DeviceProfileInfo findDeviceProfileInfoById(TenantId tenantId, UUID deviceProfileId);

    DeviceProfile save(TenantId tenantId, DeviceProfile deviceProfile);

    DeviceProfile saveAndFlush(TenantId tenantId, DeviceProfile deviceProfile);

    PageData<DeviceProfile> findDeviceProfiles(TenantId tenantId, PageLink pageLink);

    PageData<UUID> findProfileIdsByTransportType(DeviceTransportType transportType, PageLink pageLink);

    /** 跨租户枚举某传输类型的档案（含 profileData），用于监听端口等全局资源的冲突校验。 */
    PageData<DeviceProfile> findDeviceProfilesByTransportType(DeviceTransportType transportType, PageLink pageLink);

    PageData<DeviceProfileInfo> findDeviceProfileInfos(TenantId tenantId, PageLink pageLink, String transportType);

    DeviceProfile findDefaultDeviceProfile(TenantId tenantId);

    DeviceProfileInfo findDefaultDeviceProfileInfo(TenantId tenantId);

    DeviceProfile findByProvisionDeviceKey(String provisionDeviceKey);

    DeviceProfile findByName(TenantId tenantId, String profileName);

    PageData<DeviceProfile> findAllWithImages(PageLink pageLink);

    List<EntityInfo> findTenantDeviceProfileNames(UUID tenantId, boolean activeOnly);

}
