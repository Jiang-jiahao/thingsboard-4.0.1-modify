package com.jnks.iot.server.service.telemetry;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import com.jnks.iot.common.util.JnksIotThreadFactory;
import com.jnks.iot.server.cluster.JnksIotClusterService;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.queue.discovery.JnksIotApplicationEventListener;
import com.jnks.iot.server.queue.discovery.event.PartitionChangeEvent;
import com.jnks.iot.server.service.subscription.SubscriptionManagerService;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Created by ashvayka on 27.03.18.
 */
@Slf4j
public abstract class AbstractSubscriptionService extends JnksIotApplicationEventListener<PartitionChangeEvent> {

    protected final Set<TopicPartitionInfo> currentPartitions = ConcurrentHashMap.newKeySet();

    @Autowired
    protected JnksIotClusterService clusterService;
    @Autowired
    protected PartitionService partitionService;
    @Autowired
    protected Optional<SubscriptionManagerService> subscriptionManagerService;

    protected ExecutorService wsCallBackExecutor;

    protected abstract String getExecutorPrefix();

    @PostConstruct
    public void initExecutor() {
        wsCallBackExecutor = Executors.newSingleThreadExecutor(JnksIotThreadFactory.forName(getExecutorPrefix() + "-service-ws-callback"));
    }

    @PreDestroy
    public void shutdownExecutor() {
        if (wsCallBackExecutor != null) {
            wsCallBackExecutor.shutdownNow();
        }
    }

    @Override
    protected void onJnksIotApplicationEvent(PartitionChangeEvent partitionChangeEvent) {
        if (ServiceType.JNKS_IOT_CORE.equals(partitionChangeEvent.getServiceType())) {
            currentPartitions.clear();
            currentPartitions.addAll(partitionChangeEvent.getCorePartitions());
        }
    }

    /**
     * 转发到订阅管理服务，如果属于本地节点，则直接处理。不属于则转发
     * @param tenantId 租户id
     * @param entityId 实体id
     * @param toSubscriptionManagerService 订阅管理服务执行函数
     * @param toCore 消息
     */
    protected void forwardToSubscriptionManagerService(TenantId tenantId, EntityId entityId,
                                                       Consumer<SubscriptionManagerService> toSubscriptionManagerService,
                                                       Supplier<TransportProtos.ToCoreMsg> toCore) {
        TopicPartitionInfo tpi = partitionService.resolve(ServiceType.JNKS_IOT_CORE, tenantId, entityId);
        if (currentPartitions.contains(tpi)) {
            if (subscriptionManagerService.isPresent()) {
                toSubscriptionManagerService.accept(subscriptionManagerService.get());
            } else {
                log.warn("Possible misconfiguration because subscriptionManagerService is null!");
            }
        } else {
            TransportProtos.ToCoreMsg toCoreMsg = toCore.get();
            clusterService.pushMsgToCore(tpi, entityId.getId(), toCoreMsg, null);
        }
    }

    protected <T> void addWsCallback(ListenableFuture<T> saveFuture, Consumer<T> callback) {
        addCallback(saveFuture, callback, wsCallBackExecutor);
    }

    protected <T> void addCallback(ListenableFuture<T> saveFuture, Consumer<T> callback, Executor executor) {
        Futures.addCallback(saveFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(@Nullable T result) {
                callback.accept(result);
            }

            @Override
            public void onFailure(Throwable t) {}
        }, executor);
    }

    protected static Consumer<Throwable> safeCallback(FutureCallback<Void> callback) {
        if (callback != null) {
            return callback::onFailure;
        } else {
            return throwable -> {};
        }
    }

}
