package com.jnks.iot.rule.engine.aws.sqs;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

import java.util.Collections;
import java.util.Map;

@Data
public class JnksIotSqsNodeConfiguration implements NodeConfiguration<JnksIotSqsNodeConfiguration> {

    private QueueType queueType;
    private String queueUrlPattern;
    private int delaySeconds;
    private Map<String, String> messageAttributes;
    private String accessKeyId;
    private String secretAccessKey;
    private String region;

    @Override
    public JnksIotSqsNodeConfiguration defaultConfiguration() {
        JnksIotSqsNodeConfiguration configuration = new JnksIotSqsNodeConfiguration();
        configuration.setQueueType(QueueType.STANDARD);
        configuration.setQueueUrlPattern("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue-name");
        configuration.setDelaySeconds(0);
        configuration.setMessageAttributes(Collections.emptyMap());
        configuration.setRegion("us-east-1");
        return configuration;
    }

    public enum QueueType {
        STANDARD,
        FIFO
    }
}
