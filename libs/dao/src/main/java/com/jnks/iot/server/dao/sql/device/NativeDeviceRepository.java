package com.jnks.iot.server.dao.sql.device;

import org.springframework.data.domain.Pageable;
import com.jnks.iot.server.common.data.DeviceIdInfo;
import com.jnks.iot.server.common.data.ProfileEntityIdInfo;
import com.jnks.iot.server.common.data.page.PageData;

public interface NativeDeviceRepository extends NativeProfileEntityRepository {

    PageData<DeviceIdInfo> findDeviceIdInfos(Pageable pageable);

}
