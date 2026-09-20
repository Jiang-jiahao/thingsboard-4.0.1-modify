package com.jnks.iot.server.dao.ota;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.id.OtaPackageId;

@Data
@RequiredArgsConstructor
class OtaPackageCacheEvictEvent {

    private final OtaPackageId id;

}
