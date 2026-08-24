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
package org.thingsboard.server.service.edge.instructions;

import org.thingsboard.server.common.data.EdgeUpgradeInfo;
import org.thingsboard.server.common.data.edge.EdgeInstructions;
import org.thingsboard.server.common.data.id.EdgeId;
import org.thingsboard.server.common.data.id.TenantId;

import java.util.Map;

/**
 * 生成 Edge 从当前版本升级到云端版本的分步说明，并判断是否需要升级。
 */
public interface EdgeUpgradeInstructionsService {

    /** 按升级方式返回从指定 Edge 版本到当前云端版本的说明。 */
    EdgeInstructions getUpgradeInstructions(String edgeVersion, String upgradeMethod);

    /** 合并/更新版本升级路径表。 */
    void updateInstructionMap(Map<String, EdgeUpgradeInfo> upgradeVersions);

    /** 覆盖当前应用版本号。 */
    void setAppVersion(String version);

    /** 根据 Edge 上报的版本属性判断是否还有可升级路径。 */
    boolean isUpgradeAvailable(TenantId tenantId, EdgeId edgeId) throws Exception;

}
