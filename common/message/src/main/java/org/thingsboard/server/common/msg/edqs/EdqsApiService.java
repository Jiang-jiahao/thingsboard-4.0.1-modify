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
package org.thingsboard.server.common.msg.edqs;

import com.google.common.util.concurrent.ListenableFuture;
import org.thingsboard.server.common.data.edqs.query.EdqsRequest;
import org.thingsboard.server.common.data.edqs.query.EdqsResponse;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.TenantId;

/**
 * EDQS（Entity Data Query Service）查询 API 客户端接口。
 * <p>
 * 供 Core / DAO 等发起实体数据查询：将 {@link EdqsRequest} 交给 EDQS，
 * 异步拿到 {@link EdqsResponse}。真正实现通常通过队列请求-响应打到 tb-edqs
 *（如 tb-core 中的 {@code DefaultEdqsApiService}）；无真实现时由
 * {@code DummyEdqsApiService} 占位，保证 Spring 能注入。
 * <p>
 * 与 {@link EdqsService} 的分工：本接口负责<strong>读/查询</strong>；
 * {@link EdqsService} 负责<strong>写路径</strong>（实体变更后更新 EDQS 索引）。
 */
public interface EdqsApiService {

    /**
     * 向 EDQS 发起一次实体查询（过滤、选字段、分页等）。
     *
     * @param tenantId   租户
     * @param customerId 客户（权限范围）；可为 null/空视具体实现
     * @param request    查询条件
     * @return 异步查询结果
     */
    ListenableFuture<EdqsResponse> processRequest(TenantId tenantId, CustomerId customerId, EdqsRequest request);

    /**
     * 当前是否已启用 EDQS 查询 API（同步完成或手动打开后为 true）。
     */
    boolean isEnabled();

    /**
     * 运行时开关：启用或关闭 EDQS 查询 API。
     */
    void setEnabled(boolean enabled);

    /**
     * 当前部署是否支持 EDQS API（配置/能力层面，与 {@link #isEnabled()} 不同）。
     */
    boolean isSupported();

    /**
     * 数据同步完成后是否自动启用 API（对应配置如 queue.edqs.api.auto_enable）。
     */
    boolean isAutoEnable();

}
