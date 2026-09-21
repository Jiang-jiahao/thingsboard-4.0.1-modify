package com.jnks.iot.server.service.edqs;

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
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.edqs.query.EdqsRequest;
import com.jnks.iot.server.common.data.edqs.query.EdqsResponse;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.edqs.EdqsApiService;
import com.jnks.iot.server.edqs.state.EdqsPartitionService;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.gen.transport.TransportProtos.FromEdqsMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToEdqsMsg;
import com.jnks.iot.server.queue.JnksIotQueueRequestTemplate;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.provider.EdqsClientQueueFactory;
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
@ConditionalOnExpression("'${queue.edqs.api.supported:true}' == 'true'")
public class DefaultEdqsApiService implements EdqsApiService {

    private final EdqsPartitionService edqsPartitionService;
    private final EdqsClientQueueFactory queueFactory;

    /** 请求-响应模板：向 EDQS 发 ToEdqsMsg，接收 FromEdqsMsg */
    private JnksIotQueueRequestTemplate<JnksIotProtoQueueMsg<ToEdqsMsg>, JnksIotProtoQueueMsg<FromEdqsMsg>> requestTemplate;

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
        ListenableFuture<JnksIotProtoQueueMsg<FromEdqsMsg>> resultFuture = requestTemplate.send(new JnksIotProtoQueueMsg<>(key, requestMsg.build()), partition);
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
