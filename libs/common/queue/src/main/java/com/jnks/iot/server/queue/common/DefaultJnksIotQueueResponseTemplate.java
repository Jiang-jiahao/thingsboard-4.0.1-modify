package com.jnks.iot.server.queue.common;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JnksIotExecutors;
import com.jnks.iot.common.util.JnksIotThreadFactory;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.common.stats.MessagesStats;
import com.jnks.iot.server.queue.JnksIotQueueConsumer;
import com.jnks.iot.server.queue.JnksIotQueueHandler;
import com.jnks.iot.server.queue.JnksIotQueueMsg;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.JnksIotQueueResponseTemplate;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class DefaultJnksIotQueueResponseTemplate<Request extends JnksIotQueueMsg, Response extends JnksIotQueueMsg> extends AbstractJnksIotQueueTemplate
        implements JnksIotQueueResponseTemplate<Request, Response> {

    private final JnksIotQueueConsumer<Request> requestTemplate;
    private final JnksIotQueueProducer<Response> responseTemplate;
    private final ConcurrentMap<UUID, String> pendingRequests;
    private final ExecutorService loopExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final ExecutorService callbackExecutor;
    private final MessagesStats stats;
    private final int maxPendingRequests;
    private final long requestTimeout;

    private final long pollInterval;
    private volatile boolean stopped = false;
    private final AtomicInteger pendingRequestCount = new AtomicInteger();

    public static <Request extends JnksIotQueueMsg, Response extends JnksIotQueueMsg>
            DefaultJnksIotQueueResponseTemplateBuilder<Request, Response> builder() {
        return new DefaultJnksIotQueueResponseTemplateBuilder<>();
    }

    @Builder(builderMethodName = "lombokBuilder")
    public DefaultJnksIotQueueResponseTemplate(JnksIotQueueConsumer<Request> requestTemplate,
                                          JnksIotQueueProducer<Response> responseTemplate,
                                          JnksIotQueueHandler<Request, Response> handler,
                                          long pollInterval,
                                          long requestTimeout,
                                          int maxPendingRequests,
                                          ExecutorService executor,
                                          MessagesStats stats) {
        this.requestTemplate = requestTemplate;
        this.responseTemplate = responseTemplate;
        this.pendingRequests = new ConcurrentHashMap<>();
        this.maxPendingRequests = maxPendingRequests;
        this.pollInterval = pollInterval;
        this.requestTimeout = requestTimeout;
        this.callbackExecutor = executor;
        this.stats = stats;
        this.timeoutExecutor = JnksIotExecutors.newSingleThreadScheduledExecutor("jnks-iot-queue-response-template-timeout-" + requestTemplate.getTopic());
        this.loopExecutor = Executors.newSingleThreadExecutor(JnksIotThreadFactory.forName("jnks-iot-queue-response-template-loop-" + requestTemplate.getTopic()));
    }

    @Override
    public void subscribe() {
        requestTemplate.subscribe();
    }

    @Override
    public void subscribe(Set<TopicPartitionInfo> partitions) {
        requestTemplate.subscribe(partitions);
    }

    @Override
    public void launch(JnksIotQueueHandler<Request, Response> handler) {
        this.responseTemplate.init();
        loopExecutor.submit(() -> {
            while (!stopped) {
                try {
                    while (pendingRequestCount.get() >= maxPendingRequests) {
                        try {
                            Thread.sleep(pollInterval);
                        } catch (InterruptedException e) {
                            log.trace("Failed to wait until the server has capacity to handle new requests", e);
                        }
                    }
                    List<Request> requests = requestTemplate.poll(pollInterval);

                    if (requests.isEmpty()) {
                        continue;
                    }

                    requests.forEach(request -> {
                        long currentTime = System.currentTimeMillis();
                        long expireTs = bytesToLong(request.getHeaders().get(EXPIRE_TS_HEADER));
                        if (expireTs >= currentTime) {
                            byte[] requestIdHeader = request.getHeaders().get(REQUEST_ID_HEADER);
                            if (requestIdHeader == null) {
                                log.error("[{}] Missing requestId in header", request);
                                return;
                            }
                            byte[] responseTopicHeader = request.getHeaders().get(RESPONSE_TOPIC_HEADER);
                            if (responseTopicHeader == null) {
                                log.error("[{}] Missing response topic in header", request);
                                return;
                            }
                            UUID requestId = bytesToUuid(requestIdHeader);
                            String responseTopic = bytesToString(responseTopicHeader);
                            try {
                                pendingRequestCount.getAndIncrement();
                                stats.incrementTotal();
                                AsyncCallbackTemplate.withCallbackAndTimeout(handler.handle(request),
                                        response -> {
                                            pendingRequestCount.decrementAndGet();
                                            response.getHeaders().put(REQUEST_ID_HEADER, uuidToBytes(requestId));
                                            responseTemplate.send(TopicPartitionInfo.builder().topic(responseTopic).build(), response, null);
                                            stats.incrementSuccessful();
                                        },
                                        e -> {
                                            pendingRequestCount.decrementAndGet();
                                            if (e.getCause() != null && e.getCause() instanceof TimeoutException) {
                                                log.warn("[{}] Timeout to process the request: {}", requestId, request, e);
                                            } else {
                                                log.trace("[{}] Failed to process the request: {}", requestId, request, e);
                                            }
                                            stats.incrementFailed();
                                        },
                                        requestTimeout,
                                        timeoutExecutor,
                                        callbackExecutor);
                            } catch (Throwable e) {
                                pendingRequestCount.decrementAndGet();
                                log.warn("[{}] Failed to process the request: {}", requestId, request, e);
                                stats.incrementFailed();
                            }
                        }
                    });
                    requestTemplate.commit();
                } catch (Throwable e) {
                    log.warn("Failed to obtain messages from queue.", e);
                    try {
                        Thread.sleep(pollInterval);
                    } catch (InterruptedException e2) {
                        log.trace("Failed to wait until the server has capacity to handle new requests", e2);
                    }
                }
            }
        });
    }

    public void stop() {
        stopped = true;
        if (requestTemplate != null) {
            requestTemplate.unsubscribe();
        }
        if (responseTemplate != null) {
            responseTemplate.stop();
        }
        if (timeoutExecutor != null) {
            timeoutExecutor.shutdownNow();
        }
        if (loopExecutor != null) {
            loopExecutor.shutdownNow();
        }
    }

}
