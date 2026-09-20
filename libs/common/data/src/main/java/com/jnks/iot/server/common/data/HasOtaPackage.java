package com.jnks.iot.server.common.data;

import com.jnks.iot.server.common.data.id.OtaPackageId;

public interface HasOtaPackage {

    OtaPackageId getFirmwareId();

    OtaPackageId getSoftwareId();

    void setFirmwareId(OtaPackageId otaPackageId);

    void setSoftwareId(OtaPackageId otaPackageId);
}
