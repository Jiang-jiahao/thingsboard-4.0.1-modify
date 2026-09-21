package com.jnks.iot.rule.engine.kafka;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

import java.util.Collections;
import java.util.Map;

@Data
public class JnksIotKafkaNodeConfiguration implements NodeConfiguration<JnksIotKafkaNodeConfiguration> {

    private String topicPattern;
    private String keyPattern;
    private String bootstrapServers;
    private int retries;
    private int batchSize;
    private int linger;
    private int bufferMemory;
    private String acks;
    private Map<String, String> otherProperties;

    private boolean addMetadataKeyValuesAsKafkaHeaders;
    private String kafkaHeadersCharset;

    @Override
    public JnksIotKafkaNodeConfiguration defaultConfiguration() {
        JnksIotKafkaNodeConfiguration configuration = new JnksIotKafkaNodeConfiguration();
        configuration.setTopicPattern("my-topic");
        configuration.setBootstrapServers("localhost:9092");
        configuration.setRetries(0);
        configuration.setBatchSize(16384);
        configuration.setLinger(0);
        configuration.setBufferMemory(33554432);
        configuration.setAcks("-1");
        configuration.setOtherProperties(Collections.emptyMap());
        configuration.setAddMetadataKeyValuesAsKafkaHeaders(false);
        configuration.setKafkaHeadersCharset("UTF-8");
        return configuration;
    }
}
