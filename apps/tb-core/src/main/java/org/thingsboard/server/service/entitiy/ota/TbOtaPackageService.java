/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
