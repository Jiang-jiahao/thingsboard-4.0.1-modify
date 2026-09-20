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
import com.jnks.iot.server.queue.TbQueueConsumer;
import com.jnks.iot.server.queue.TbQueueProducer;
import com.jnks.iot.server.queue.TbQueueResponseTemplate;
import com.jnks.iot.server.queue.common.DefaultTbQueueResponseTemplate;
import com.jnks.iot.server.queue.common.TbProtoQueueMsg;
import com.jnks.iot.server.queue.provider.TbCoreQueueFactory;
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
public class TbCoreTransportApiService {
    private final TbCoreQueueFactory tbCoreQueueFactory;
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
    private TbQueueResponseTemplate<TbProtoQueueMsg<TransportApiRequestMsg>,
            TbProtoQueueMsg<TransportApiResponseMsg>> transportApiTemplate;

    public TbCoreTransportApiService(TbCoreQueueFactory tbCoreQueueFactory, TransportApiService transportApiService, StatsFactory statsFactory) {
        this.tbCoreQueueFactory = tbCoreQueueFactory;
        this.transportApiService = transportApiService;
        this.statsFactory = statsFactory;
    }

    /**
     * 组装请求/响应模板：创建消费者、生产者与回调线程池。
     */
    @PostConstruct
    public void init() {
        this.transportCallbackExecutor = JnksIotExecutors.newWorkStealingPool(maxCallbackThreads, getClass());
        TbQueueProducer<TbProtoQueueMsg<TransportApiResponseMsg>> producer = tbCoreQueueFactory.createTransportApiResponseProducer();
        TbQueueConsumer<TbProtoQueueMsg<TransportApiRequestMsg>> consumer = tbCoreQueueFactory.createTransportApiRequestConsumer();

        String key = StatsType.TRANSPORT.getName();
        MessagesStats queueStats = statsFactory.createMessagesStats(key);

        DefaultTbQueueResponseTemplate.DefaultTbQueueResponseTemplateBuilder
                <TbProtoQueueMsg<TransportApiRequestMsg>, TbProtoQueueMsg<TransportApiResponseMsg>> builder = DefaultTbQueueResponseTemplate.builder();
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
