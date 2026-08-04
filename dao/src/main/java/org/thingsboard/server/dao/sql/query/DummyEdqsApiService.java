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
package org.thingsboard.server.dao.sql.query;

import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.edqs.query.EdqsRequest;
import org.thingsboard.server.common.data.edqs.query.EdqsResponse;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.edqs.EdqsApiService;

/**
 * {@link EdqsApiService} 的占位实现：容器中尚无真正的 EDQS 查询客户端时使用。
 * <p>
 * 放在 dao 模块，是因为 DAO（如实体查询）需要注入 {@link EdqsApiService}，
 * 又不能依赖 application 层的真实现；用 {@link ConditionalOnMissingBean} 保证：
 * 有 {@code DefaultEdqsApiService} 等真 Bean 时自动让位，否则仍能启动。
 * <p>
 * 行为：{@link #isSupported()}/{@link #isEnabled()} 恒为 false；
 * {@link #processRequest} 直接抛 {@link UnsupportedOperationException}。
 */
@Service
@Slf4j
@ConditionalOnMissingBean(value = EdqsApiService.class, ignored = DummyEdqsApiService.class)
public class DummyEdqsApiService implements EdqsApiService {

    @Override
    public ListenableFuture<EdqsResponse> processRequest(TenantId tenantId, CustomerId customerId, EdqsRequest request) {
        // 未接入 EDQS 时不允许走查询 API
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public void setEnabled(boolean enabled) {
        log.warn("Got request to enable EDQS API, but it isn't supported", new RuntimeException("stacktrace"));
    }

    @Override
    public boolean isSupported() {
        return false;
    }

    @Override
    public boolean isAutoEnable() {
        return false;
    }

}
