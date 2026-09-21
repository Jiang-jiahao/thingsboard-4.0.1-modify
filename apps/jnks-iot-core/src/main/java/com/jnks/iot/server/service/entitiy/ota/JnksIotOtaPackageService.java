package com.jnks.iot.server.service.entitiy.ota;

import com.jnks.iot.server.common.data.OtaPackageInfo;
import com.jnks.iot.server.common.data.SaveOtaPackageInfoRequest;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.ota.ChecksumAlgorithm;

/**
 * OTA 固件包业务层契约：保存元数据、上传数据包与删除。
 * <p>
 * 由 OtaPackageController 调用；实现类委托 DAO 并写审计日志。
 */
public interface JnksIotOtaPackageService {

    /** 保存 OTA 包元信息。 */
    OtaPackageInfo save(SaveOtaPackageInfoRequest saveOtaPackageInfoRequest, User user) throws JnksIotException;

    /** 保存 OTA 包二进制数据与校验和。 */
    OtaPackageInfo saveOtaPackageData(OtaPackageInfo otaPackageInfo, String checksum, ChecksumAlgorithm checksumAlgorithm,
                                      byte[] data, String filename, String contentType, User user) throws JnksIotException;

    /** 删除 OTA 包。 */
    void delete(OtaPackageInfo otaPackageInfo, User user) throws JnksIotException;

}
