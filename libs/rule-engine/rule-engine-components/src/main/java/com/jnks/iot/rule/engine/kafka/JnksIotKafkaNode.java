package com.jnks.iot.rule.engine.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.util.ReflectionUtils;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.external.JnksIotAbstractExternalNode;
import com.jnks.iot.server.common.data.exception.JnksIotKafkaClientError;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

@Slf4j
@RuleNode(
        type = ComponentType.EXTERNAL,
        name = "Kafka",
        configClazz = JnksIotKafkaNodeConfiguration.class,
        version = 1,
        nodeDescription = "将消息发布到 Kafka 服务器",
        nodeDetails = "将通过 Kafka 生产者向 Kafka 服务器发送记录。 " +
                "出站消息将包含响应字段（<code>offset</code>、<code>partition</code>、<code>topic</code>）" +
                " 这些字段来自 Kafka，位于消息元数据中。例如，可通过 <code>metadata.partition</code> 访问 <b>partition</b> 字段。",
        configDirective = "jnksIotExternalNodeKafkaConfig",
        iconUrl = "data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTUzOCIgaGVpZ2h0PSIyNTAwIiB2aWV3Qm94PSIwIDAgMjU2IDQxNiIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIiBwcmVzZXJ2ZUFzcGVjdFJhdGlvPSJ4TWlkWU1pZCI+PHBhdGggZD0iTTIwMS44MTYgMjMwLjIxNmMtMTYuMTg2IDAtMzAuNjk3IDcuMTcxLTQwLjYzNCAxOC40NjFsLTI1LjQ2My0xOC4wMjZjMi43MDMtNy40NDIgNC4yNTUtMTUuNDMzIDQuMjU1LTIzLjc5NyAwLTguMjE5LTEuNDk4LTE2LjA3Ni00LjExMi0yMy40MDhsMjUuNDA2LTE3LjgzNWM5LjkzNiAxMS4yMzMgMjQuNDA5IDE4LjM2NSA0MC41NDggMTguMzY1IDI5Ljg3NSAwIDU0LjE4NC0yNC4zMDUgNTQuMTg0LTU0LjE4NCAwLTI5Ljg3OS0yNC4zMDktNTQuMTg0LTU0LjE4NC01NC4xODQtMjkuODc1IDAtNTQuMTg0IDI0LjMwNS01NC4xODQgNTQuMTg0IDAgNS4zNDguODA4IDEwLjUwNSAyLjI1OCAxNS4zODlsLTI1LjQyMyAxNy44NDRjLTEwLjYyLTEzLjE3NS0yNS45MTEtMjIuMzc0LTQzLjMzMy0yNS4xODJ2LTMwLjY0YzI0LjU0NC01LjE1NSA0My4wMzctMjYuOTYyIDQzLjAzNy01My4wMTlDMTI0LjE3MSAyNC4zMDUgOTkuODYyIDAgNjkuOTg3IDAgNDAuMTEyIDAgMTUuODAzIDI0LjMwNSAxNS44MDMgNTQuMTg0YzAgMjUuNzA4IDE4LjAxNCA0Ny4yNDYgNDIuMDY3IDUyLjc2OXYzMS4wMzhDMjUuMDQ0IDE0My43NTMgMCAxNzIuNDAxIDAgMjA2Ljg1NGMwIDM0LjYyMSAyNS4yOTIgNjMuMzc0IDU4LjM1NSA2OC45NHYzMi43NzRjLTI0LjI5OSA1LjM0MS00Mi41NTIgMjcuMDExLTQyLjU1MiA1Mi44OTQgMCAyOS44NzkgMjQuMzA5IDU0LjE4NCA1NC4xODQgNTQuMTg0IDI5Ljg3NSAwIDU0LjE4NC0yNC4zMDUgNTQuMTg0LTU0LjE4NCAwLTI1Ljg4My0xOC4yNTMtNDcuNTUzLTQyLjU1Mi01Mi44OTR2LTMyLjc3NWE2OS45NjUgNjkuOTY1IDAgMCAwIDQyLjYtMjQuNzc2bDI1LjYzMyAxOC4xNDNjLTEuNDIzIDQuODQtMi4yMiA5Ljk0Ni0yLjIyIDE1LjI0IDAgMjkuODc5IDI0LjMwOSA1NC4xODQgNTQuMTg0IDU0LjE4NCAyOS44NzUgMCA1NC4xODQtMjQuMzA1IDU0LjE4NC01NC4xODQgMC0yOS44NzktMjQuMzA5LTU0LjE4NC01NC4xODQtNTQuMTg0em0wLTEyNi42OTVjMTQuNDg3IDAgMjYuMjcgMTEuNzg4IDI2LjI3IDI2LjI3MXMtMTEuNzgzIDI2LjI3LTI2LjI3IDI2LjI3LTI2LjI3LTExLjc4Ny0yNi4yNy0yNi4yN2MwLTE0LjQ4MyAxMS43ODMtMjYuMjcxIDI2LjI3LTI2LjI3MXptLTE1OC4xLTQ5LjMzN2MwLTE0LjQ4MyAxMS43ODQtMjYuMjcgMjYuMjcxLTI2LjI3czI2LjI3IDExLjc4NyAyNi4yNyAyNi4yN2MwIDE0LjQ4My0xMS43ODMgMjYuMjctMjYuMjcgMjYuMjdzLTI2LjI3MS0xMS43ODctMjYuMjcxLTI2LjI3em01Mi41NDEgMzA3LjI3OGMwIDE0LjQ4My0xMS43ODMgMjYuMjctMjYuMjcgMjYuMjdzLTI2LjI3MS0xMS43ODctMjYuMjcxLTI2LjI3YzAtMTQuNDgzIDExLjc4NC0yNi4yNyAyNi4yNzEtMjYuMjdzMjYuMjcgMTEuNzg3IDI2LjI3IDI2LjI3em0tMjYuMjcyLTExNy45N2MtMjAuMjA1IDAtMzYuNjQyLTE2LjQzNC0zNi42NDItMzYuNjM4IDAtMjAuMjA1IDE2LjQzNy0zNi42NDIgMzYuNjQyLTM2LjY0MiAyMC4yMDQgMCAzNi42NDEgMTYuNDM3IDM2LjY0MSAzNi42NDIgMCAyMC4yMDQtMTYuNDM3IDM2LjYzOC0zNi42NDEgMzYuNjM4em0xMzEuODMxIDY3LjE3OWMtMTQuNDg3IDAtMjYuMjctMTEuNzg4LTI2LjI3LTI2LjI3MXMxMS43ODMtMjYuMjcgMjYuMjctMjYuMjcgMjYuMjcgMTEuNzg3IDI2LjI3IDI2LjI3YzAgMTQuNDgzLTExLjc4MyAyNi4yNzEtMjYuMjcgMjYuMjcxeiIvPjwvc3ZnPg=="
)
public class JnksIotKafkaNode extends JnksIotAbstractExternalNode {

    private static final String OFFSET = "offset";
    private static final String PARTITION = "partition";
    private static final String TOPIC = "topic";
    private static final String ERROR = "error";
    public static final String JNKS_IOT_MSG_MD_PREFIX = "jnks_iot_msg_md_";
    private static final Field IO_THREAD_FIELD = ReflectionUtils.findField(KafkaProducer.class, "ioThread");

    static {
        IO_THREAD_FIELD.setAccessible(true);
    }

    private JnksIotKafkaNodeConfiguration config;
    private boolean addMetadataKeyValuesAsKafkaHeaders;
    private Charset toBytesCharset;

    private Producer<String, String> producer;
    private Throwable initError;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        super.init(ctx);
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotKafkaNodeConfiguration.class);
        this.initError = null;
        Properties properties = new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-tb-kafka-node-" + ctx.getSelfId().getId().toString() + "-" + ctx.getServiceId());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, config.getAcks());
        properties.put(ProducerConfig.RETRIES_CONFIG, config.getRetries());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getBatchSize());
        properties.put(ProducerConfig.LINGER_MS_CONFIG, config.getLinger());
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getBufferMemory());
        if (config.getOtherProperties() != null) {
            config.getOtherProperties().forEach((k, v) -> {
                if (SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG.equals(k)
                        || SslConfigs.SSL_KEYSTORE_KEY_CONFIG.equals(k)
                        || SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG.equals(k)) {
                    v = v.replace("\\n", "\n");
                }
                properties.put(k, v);
            });
        }
        addMetadataKeyValuesAsKafkaHeaders = BooleanUtils.toBooleanDefaultIfNull(config.isAddMetadataKeyValuesAsKafkaHeaders(), false);
        toBytesCharset = config.getKafkaHeadersCharset() != null ? Charset.forName(config.getKafkaHeadersCharset()) : StandardCharsets.UTF_8;
        try {
            this.producer = getKafkaProducer(properties);
            Thread ioThread = (Thread) ReflectionUtils.getField(IO_THREAD_FIELD, producer);
            ioThread.setUncaughtExceptionHandler((thread, throwable) -> {
                if (throwable instanceof JnksIotKafkaClientError) {
                    initError = throwable;
                    destroy();
                }
            });
        } catch (Exception e) {
            throw new JnksIotNodeException(e);
        }
    }

    KafkaProducer<String, String> getKafkaProducer(Properties properties) {
        return new KafkaProducer<>(properties);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        String topic = JnksIotNodeUtils.processPattern(config.getTopicPattern(), msg);
        String keyPattern = config.getKeyPattern();
        var jnksIotMsg = ackIfNeeded(ctx, msg);
        try {
            if (initError != null) {
                ctx.tellFailure(jnksIotMsg, new RuntimeException("Failed to initialize Kafka rule node producer: " + initError.getMessage()));
            } else {
                ctx.getExternalCallExecutor().executeAsync(() -> {
                    publish(
                            ctx,
                            jnksIotMsg,
                            topic,
                            keyPattern == null || keyPattern.isEmpty()
                                    ? null
                                    : JnksIotNodeUtils.processPattern(config.getKeyPattern(), jnksIotMsg)
                    );
                    return null;
                });
            }
        } catch (Exception e) {
            ctx.tellFailure(jnksIotMsg, e);
        }
    }

    protected void publish(JnksIotContext ctx, JnksIotMsg msg, String topic, String key) {
        try {
            if (!addMetadataKeyValuesAsKafkaHeaders) {
                //TODO: external system executor
                producer.send(new ProducerRecord<>(topic, key, msg.getData()),
                        (metadata, e) -> processRecord(ctx, msg, metadata, e));
            } else {
                Headers headers = new RecordHeaders();
                msg.getMetaData().values().forEach((k, v) -> headers.add(new RecordHeader(JNKS_IOT_MSG_MD_PREFIX + k, v.getBytes(toBytesCharset))));
                producer.send(new ProducerRecord<>(topic, null, null, key, msg.getData(), headers),
                        (metadata, e) -> processRecord(ctx, msg, metadata, e));
            }
        } catch (Exception e) {
            log.debug("[{}] Failed to process message: {}", ctx.getSelfId(), msg, e);
        }
    }

    @Override
    public void destroy() {
        if (this.producer != null) {
            try {
                this.producer.close();
            } catch (Exception e) {
                log.error("Failed to close producer during destroy()", e);
            }
        }
    }

    private void processRecord(JnksIotContext ctx, JnksIotMsg msg, RecordMetadata metadata, Exception e) {
        if (e == null) {
            tellSuccess(ctx, processResponse(msg, metadata));
        } else {
            tellFailure(ctx, processException(msg, e), e);
        }
    }

    private JnksIotMsg processResponse(JnksIotMsg origMsg, RecordMetadata recordMetadata) {
        JnksIotMsgMetaData metaData = origMsg.getMetaData().copy();
        metaData.putValue(OFFSET, String.valueOf(recordMetadata.offset()));
        metaData.putValue(PARTITION, String.valueOf(recordMetadata.partition()));
        metaData.putValue(TOPIC, recordMetadata.topic());
        return origMsg.transform()
                .metaData(metaData)
                .build();
    }

    private JnksIotMsg processException(JnksIotMsg origMsg, Exception e) {
        JnksIotMsgMetaData metaData = origMsg.getMetaData().copy();
        metaData.putValue(ERROR, e.getClass() + ": " + e.getMessage());
        return origMsg.transform()
                .metaData(metaData)
                .build();
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        boolean hasChanges = false;
        switch (fromVersion) {
            case 0 -> {
                if (oldConfiguration.has("keySerializer") || oldConfiguration.has("valueSerializer")) {
                    ObjectNode objectConfiguration = (ObjectNode) oldConfiguration;
                    objectConfiguration.remove("keySerializer");
                    objectConfiguration.remove("valueSerializer");
                    hasChanges = true;
                }
            }
            default -> {
            }
        }
        return new JnksIotPair<>(hasChanges, oldConfiguration);
    }

}
