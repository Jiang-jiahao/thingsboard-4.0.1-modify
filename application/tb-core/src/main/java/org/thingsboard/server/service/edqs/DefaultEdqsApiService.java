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
package org.thingsboard.server.service.edqs;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.common.data.edqs.query.EdqsRequest;
import org.thingsboard.server.common.data.edqs.query.EdqsResponse;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.edqs.EdqsApiService;
import org.thingsboard.server.edqs.state.EdqsPartitionService;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.FromEdqsMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToEdqsMsg;
import org.thingsboard.server.queue.TbQueueRequestTemplate;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.provider.EdqsClientQueueFactory;
import org.thingsboard.server.queue.util.TbCoreComponent;

import java.util.UUID;

/**
 * EDQS 查询 API 默认实现（Core 侧）。
 * <p>
 * 将实体数据查询请求封装为队列消息，按租户分区发送到 EDQS，并异步等待响应。
 * 仅在 {@code queue.edqs.api.supported=true} 时生效。
 */
@Service
@Slf4j
@RequiredArgsConstructor
@TbCoreComponent
@ConditionalOnExpression("'${queue.edqs.api.supported:true}' == 'true'")
public class DefaultEdqsApiService implements EdqsApiService {

    private final EdqsPartitionService edqsPartitionService;
    private final EdqsClientQueueFactory queueFactory;

    /** 请求-响应模板：向 EDQS 发 ToEdqsMsg，接收 FromEdqsMsg */
    private TbQueueRequestTemplate<TbProtoQueueMsg<ToEdqsMsg>, TbProtoQueueMsg<FromEdqsMsg>> requestTemplate;

    /** 全量同步完成后是否自动开启 API */
    @Value("${queue.edqs.api.auto_enable:true}")
    private boolean autoEnable;

    /** API 当前是否可用；null 表示尚未设置 */
    private Boolean apiEnabled = null;

    @PostConstruct
    private void init() {
        requestTemplate = queueFactory.createEdqsRequestTemplate();
        requestTemplate.init();
    }

    /**
     * 向 EDQS 发起实体查询请求。
     *
     * @param tenantId   租户 ID
     * @param customerId 客户 ID（可为空）
     * @param request    查询条件
     * @return 异步查询结果
     */
    @Override
    public ListenableFuture<EdqsResponse> processRequest(TenantId tenantId, CustomerId customerId, EdqsRequest request) {
        var requestMsg = ToEdqsMsg.newBuilder()
                .setTenantIdMSB(tenantId.getId().getMostSignificantBits())
                .setTenantIdLSB(tenantId.getId().getLeastSignificantBits())
                .setTs(System.currentTimeMillis())
                .setRequestMsg(TransportProtos.EdqsRequestMsg.newBuilder()
                        .setValue(JacksonUtil.toString(request))
                        .build());
        if (customerId != null && !customerId.isNullUid()) {
            requestMsg.setCustomerIdMSB(customerId.getId().getMostSignificantBits());
            requestMsg.setCustomerIdLSB(customerId.getId().getLeastSignificantBits());
        }

        UUID key = UUID.randomUUID();
        Integer partition = edqsPartitionService.resolvePartition(tenantId, key);
        ListenableFuture<TbProtoQueueMsg<FromEdqsMsg>> resultFuture = requestTemplate.send(new TbProtoQueueMsg<>(key, requestMsg.build()), partition);
        return Futures.transform(resultFuture, msg -> {
            TransportProtos.EdqsResponseMsg responseMsg = msg.getValue().getResponseMsg();
            return JacksonUtil.fromString(responseMsg.getValue(), EdqsResponse.class);
        }, MoreExecutors.directExecutor());
    }

    /** API 是否已启用（可用于对外查询） */
    @Override
    public boolean isEnabled() {
        return Boolean.TRUE.equals(apiEnabled);
    }

    /** 启用或禁用 EDQS API */
    @Override
    public void setEnabled(boolean enabled) {
        if (enabled) {
            log.info("Enabling EDQS API");
        } else {
            log.info("Disabling EDQS API");
        }
        apiEnabled = enabled;
    }

    /** 当前部署是否支持 EDQS API（本实现恒为 true） */
    @Override
    public boolean isSupported() {
        return true;
    }

    /** 同步完成后是否自动启用 API */
    @Override
    public boolean isAutoEnable() {
        return autoEnable;
    }

    @PreDestroy
    private void stop() {
        requestTemplate.stop();
    }

}
