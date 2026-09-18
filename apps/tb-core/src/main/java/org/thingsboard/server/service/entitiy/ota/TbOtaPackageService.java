package org.thingsboard.server.service.entitiy.ota;

import org.thingsboard.server.common.data.OtaPackageInfo;
import org.thingsboard.server.common.data.SaveOtaPackageInfoRequest;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.ota.ChecksumAlgorithm;

/**
 * OTA 固件包业务层契约：保存元数据、上传数据包与删除。
 * <p>
 * 由 OtaPackageController 调用；实现类委托 DAO 并写审计日志。
 */
public interface TbOtaPackageService {

    /** 保存 OTA 包元信息。 */
    OtaPackageInfo save(SaveOtaPackageInfoRequest saveOtaPackageInfoRequest, User user) throws ThingsboardException;

    /** 保存 OTA 包二进制数据与校验和。 */
    OtaPackageInfo saveOtaPackageData(OtaPackageInfo otaPackageInfo, String checksum, ChecksumAlgorithm checksumAlgorithm,
                                      byte[] data, String filename, String contentType, User user) throws ThingsboardException;

    /** 删除 OTA 包。 */
    void delete(OtaPackageInfo otaPackageInfo, User user) throws ThingsboardException;

}
