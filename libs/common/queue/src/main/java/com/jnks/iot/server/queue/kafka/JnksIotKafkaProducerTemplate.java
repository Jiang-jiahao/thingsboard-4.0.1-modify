package com.jnks.iot.server.queue.kafka;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.queue.JnksIotQueueAdmin;
import com.jnks.iot.server.queue.JnksIotQueueCallback;
import com.jnks.iot.server.queue.JnksIotQueueMsg;
import com.jnks.iot.server.queue.JnksIotQueueProducer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by ashvayka on 24.09.18.
 */
@Slf4j
public class JnksIotKafkaProducerTemplate<T extends JnksIotQueueMsg> implements JnksIotQueueProducer<T> {

    private final KafkaProducer<String, byte[]> producer;

    @Getter
    private final String defaultTopic;

    @Getter
    private final JnksIotKafkaSettings settings;

    private final JnksIotQueueAdmin admin;

    private final Set<String> topics;

    @Getter
    private final String clientId;

    /**
     * IDEA cannot infer Lombok's generated {@code builder()} against a generic class.
     * Keep a handwritten factory so assignment-site type inference works.
     */
    public static <T extends JnksIotQueueMsg> JnksIotKafkaProducerTemplateBuilder<T> builder() {
        return new JnksIotKafkaProducerTemplateBuilder<>();
    }

    @Builder(builderMethodName = "lombokBuilder")
    private JnksIotKafkaProducerTemplate(JnksIotKafkaSettings settings, String defaultTopic, String clientId, JnksIotQueueAdmin admin) {
        Properties props = settings.toProducerProps();

        this.clientId = Objects.requireNonNull(clientId, "Kafka producer client.id is null");
        if (!StringUtils.isEmpty(clientId)) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        }
        this.settings = settings;

        this.producer = new KafkaProducer<>(props);
        this.defaultTopic = defaultTopic;
        this.admin = admin;
        topics = ConcurrentHashMap.newKeySet();
    }

    @Override
    public void init() {
    }

    void addAnalyticHeaders(List<Header> headers) {
        headers.add(new RecordHeader("_producerId", getClientId().getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("_threadName", Thread.currentThread().getName().getBytes(StandardCharsets.UTF_8)));
        if (log.isTraceEnabled()) {
            try {
                StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
                int maxLevel = Math.min(stackTrace.length, 20);
                for (int i = 2; i < maxLevel; i++) { // ignore two levels: getStackTrace and addAnalyticHeaders
                    headers.add(new RecordHeader("_stackTrace" + i, stackTrace[i].toString().getBytes(StandardCharsets.UTF_8)));
                }
            } catch (Throwable t) {
                log.trace("Failed to add stacktrace headers in Kafka producer {}", getClientId(), t);
            }
        }
    }

    @Override
    public void send(TopicPartitionInfo tpi, T msg, JnksIotQueueCallback callback) {
        send(tpi, msg.getKey().toString(), msg, callback);
    }

    public void send(TopicPartitionInfo tpi, String key, T msg, JnksIotQueueCallback callback) {
        try {
            String topic = tpi.getFullTopicName();
            createTopicIfNotExist(topic);
            byte[] data = msg.getData();
            ProducerRecord<String, byte[]> record;
            List<Header> headers = msg.getHeaders().getData().entrySet().stream().map(e -> new RecordHeader(e.getKey(), e.getValue())).collect(Collectors.toList());
            if (log.isDebugEnabled()) {
                addAnalyticHeaders(headers);
            }
            Integer partition = tpi.isUseInternalPartition() ? tpi.getPartition().orElse(null) : null;
            // 如果未指定分区，则会根据该key的hash值进行分区路由
            record = new ProducerRecord<>(topic, partition, key, data, headers);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    if (callback != null) {
                        callback.onSuccess(new KafkaJnksIotQueueMsgMetadata(metadata));
                    }
                } else {
                    if (callback != null) {
                        callback.onFailure(exception);
                    } else {
                        log.warn("Producer template failure", exception);
                    }
                }
            });
        } catch (Exception e) {
            if (callback != null) {
                callback.onFailure(e);
            } else {
                log.warn("Producer template failure (send method wrapper): {}", e.getMessage(), e);
            }
            throw e;
        }
    }

    private void createTopicIfNotExist(String topic) {
        if (topics.contains(topic)) {
            return;
        }
        admin.createTopicIfNotExists(topic);
        topics.add(topic);
    }

    @Override
    public void stop() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }

    @Override
    public void flush() {
        if (producer != null) {
            producer.flush();
        }
    }

}
