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

import jakarta.servlet.http.HttpServletRequest;
import org.thingsboard.server.common.data.edge.Edge;
import org.thingsboard.server.common.data.edge.EdgeInstructions;

/**
 * 生成指定 Edge 的安装说明（Docker / Ubuntu / CentOS）。
 * <p>
 * 由 Edge 相关 Controller 调用，根据安装方式返回已替换路由密钥、RPC 端口等占位符的 Markdown。
 */
public interface EdgeInstallInstructionsService {

    /** 按安装方式返回该 Edge 的安装说明文本。 */
    EdgeInstructions getInstallInstructions(Edge edge, String installationMethod, HttpServletRequest request);

    /** 覆盖当前应用版本号（用于模板中的 Edge 镜像标签）。 */
    void setAppVersion(String version);

}
