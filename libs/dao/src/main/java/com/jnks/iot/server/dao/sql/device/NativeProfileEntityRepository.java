package com.jnks.iot.server.dao.sql.device;

import org.springframework.data.domain.Pageable;
import com.jnks.iot.server.common.data.ProfileEntityIdInfo;
import com.jnks.iot.server.common.data.page.PageData;

import java.util.UUID;

public interface NativeProfileEntityRepository {

    PageData<ProfileEntityIdInfo> findProfileEntityIdInfos(Pageable pageable);

    PageData<ProfileEntityIdInfo> findProfileEntityIdInfosByTenantId(UUID tenantId, Pageable pageable);

}
