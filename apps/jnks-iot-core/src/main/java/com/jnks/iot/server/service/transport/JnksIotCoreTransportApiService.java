package com.jnks.iot.server.service.transport;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.JnksIotExecutors;
import com.jnks.iot.server.common.stats.MessagesStats;
import com.jnks.iot.server.common.stats.StatsFactory;
import com.jnks.iot.server.common.stats.StatsType;
import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiRequestMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiResponseMsg;
import com.jnks.iot.server.queue.JnksIotQueueConsumer;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.JnksIotQueueResponseTemplate;
import com.jnks.iot.server.queue.common.DefaultJnksIotQueueResponseTemplate;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.provider.JnksIotCoreQueueFactory;
import com.jnks.iot.common.util.AfterStartUp;
import java.util.concurrent.ExecutorService;

/**
 * Core 侧 Transport API 请求消费入口。
 * <p>
 * 订阅 Transport API 请求队列，将凭证校验、设备查询、OTA 等请求交给 {@link TransportApiService}，
 * 再把响应写回 Transport 节点。应用就绪后启动 poll。
 */
@Slf4j
@Service
public class JnksIotCoreTransportApiService {
    private final JnksIotCoreQueueFactory jnksIotCoreQueueFactory;
    private final TransportApiService transportApiService;
    private final StatsFactory statsFactory;

    @Value("${queue.transport_api.max_pending_requests:10000}")
    private int maxPendingRequests;
    @Value("${queue.transport_api.max_requests_timeout:10000}")
    private long requestTimeout;
    @Value("${queue.transport_api.request_poll_interval:25}")
    private int responsePollDuration;
    @Value("${queue.transport_api.max_callback_threads:100}")
    private int maxCallbackThreads;

    private ExecutorService transportCallbackExecutor;
    private JnksIotQueueResponseTemplate<JnksIotProtoQueueMsg<TransportApiRequestMsg>,
            JnksIotProtoQueueMsg<TransportApiResponseMsg>> transportApiTemplate;

    public JnksIotCoreTransportApiService(JnksIotCoreQueueFactory jnksIotCoreQueueFactory, TransportApiService transportApiService, StatsFactory statsFactory) {
        this.jnksIotCoreQueueFactory = jnksIotCoreQueueFactory;
        this.transportApiService = transportApiService;
        this.statsFactory = statsFactory;
    }

    /**
     * 组装请求/响应模板：创建消费者、生产者与回调线程池。
     */
    @PostConstruct
    public void init() {
        this.transportCallbackExecutor = JnksIotExecutors.newWorkStealingPool(maxCallbackThreads, getClass());
        JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportApiResponseMsg>> producer = jnksIotCoreQueueFactory.createTransportApiResponseProducer();
        JnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportApiRequestMsg>> consumer = jnksIotCoreQueueFactory.createTransportApiRequestConsumer();

        String key = StatsType.TRANSPORT.getName();
        MessagesStats queueStats = statsFactory.createMessagesStats(key);

        DefaultJnksIotQueueResponseTemplate.DefaultJnksIotQueueResponseTemplateBuilder
                <JnksIotProtoQueueMsg<TransportApiRequestMsg>, JnksIotProtoQueueMsg<TransportApiResponseMsg>> builder = DefaultJnksIotQueueResponseTemplate.builder();
        builder.requestTemplate(consumer);
        builder.responseTemplate(producer);
        builder.maxPendingRequests(maxPendingRequests);
        builder.requestTimeout(requestTimeout);
        builder.pollInterval(responsePollDuration);
        builder.executor(transportCallbackExecutor);
        builder.handler(transportApiService);
        builder.stats(queueStats);
        transportApiTemplate = builder.build();
    }

    /**
     * 应用就绪后订阅并启动 Transport API 请求消费。
     */
    @AfterStartUp(order = AfterStartUp.REGULAR_SERVICE)
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        log.info("Received application ready event. Starting polling for events.");
        transportApiTemplate.subscribe();
        transportApiTemplate.launch(transportApiService);
    }

    /**
     * 停止请求模板与回调线程池。
     */
    @PreDestroy
    public void destroy() {
        if (transportApiTemplate != null) {
            transportApiTemplate.stop();
        }
        if (transportCallbackExecutor != null) {
            transportCallbackExecutor.shutdownNow();
        }
    }

}
