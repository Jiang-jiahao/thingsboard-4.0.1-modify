package com.jnks.iot.server.service.ruleengine;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.JnksIotExecutors;
import com.jnks.iot.server.cluster.JnksIotClusterService;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.queue.JnksIotCallback;
import com.jnks.iot.server.common.msg.queue.JnksIotMsgCallback;
import com.jnks.iot.server.gen.transport.TransportProtos;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * {@link RuleEngineCallService} 默认实现。
 * <p>
 * 本机用 {@link ConcurrentHashMap} 保存 requestId → 消费者；超时调度线程按消息元数据 expirationTime 清理陈旧等待。
 *
 * @see RuleEngineCallService
 */
@Service
@Slf4j
public class DefaultRuleEngineCallService implements RuleEngineCallService {

    private final JnksIotClusterService clusterService;

    private ScheduledExecutorService executor;

    private final ConcurrentMap<UUID, Consumer<JnksIotMsg>> requests = new ConcurrentHashMap<>();

    public DefaultRuleEngineCallService(JnksIotClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * 启动 REST 回调超时调度线程。
     */
    @PostConstruct
    public void initExecutor() {
        executor = JnksIotExecutors.newSingleThreadScheduledExecutor("re-rest-callback");
    }

    /**
     * 关闭超时调度线程。
     */
    @PreDestroy
    public void shutdownExecutor() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    /**
     * 登记回调、推入规则引擎并调度超时。
     */
    @Override
    public void processRestApiCallToRuleEngine(TenantId tenantId, UUID requestId, JnksIotMsg request, boolean useQueueFromJnksIotMsg, Consumer<JnksIotMsg> responseConsumer) {
        log.trace("[{}] Processing REST API call to rule engine: [{}] for entity: [{}]", tenantId, requestId, request.getOriginator());
        requests.put(requestId, responseConsumer);
        sendRequestToRuleEngine(tenantId, request, useQueueFromJnksIotMsg);
        scheduleTimeout(request, requestId, requests);
    }

    /**
     * 匹配本机等待的 REST 回调并 ack 队列消息。
     */
    @Override
    public void onQueueMsg(TransportProtos.RestApiCallResponseMsgProto restApiCallResponseMsg, JnksIotCallback callback) {
        UUID requestId = new UUID(restApiCallResponseMsg.getRequestIdMSB(), restApiCallResponseMsg.getRequestIdLSB());
        Consumer<JnksIotMsg> consumer = requests.remove(requestId);
        if (consumer != null) {
            consumer.accept(JnksIotMsg.fromBytes(null, restApiCallResponseMsg.getResponse().toByteArray(), JnksIotMsgCallback.EMPTY));
        } else {
            log.trace("[{}] Unknown or stale rest api call response received", requestId);
        }
        callback.onSuccess();
    }

    private void sendRequestToRuleEngine(TenantId tenantId, JnksIotMsg msg, boolean useQueueFromJnksIotMsg) {
        clusterService.pushMsgToRuleEngine(tenantId, msg.getOriginator(), msg, useQueueFromJnksIotMsg, null);
    }

    private void scheduleTimeout(JnksIotMsg request, UUID requestId, ConcurrentMap<UUID, Consumer<JnksIotMsg>> requestsMap) {
        long expirationTime = Long.parseLong(request.getMetaData().getValue("expirationTime"));
        long timeout = Math.max(0, expirationTime - System.currentTimeMillis());
        log.trace("[{}] processing the request: [{}]", this.hashCode(), requestId);
        executor.schedule(() -> {
            Consumer<JnksIotMsg> consumer = requestsMap.remove(requestId);
            if (consumer != null) {
                log.trace("[{}] request timeout detected: [{}]", this.hashCode(), requestId);
                consumer.accept(null);
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }
}
