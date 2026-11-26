package com.jnks.iot.rule.engine.rocketmq;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

@Data
public class JnksIotRocketMqNodeConfiguration implements NodeConfiguration<JnksIotRocketMqNodeConfiguration> {

    private String topicPattern;
    private String keyPattern;
    private String tagPattern;
    private String nameServerAddress;
    private String producerGroup;
    private String sendMode;
    private int sendMsgTimeout;
    private int retryTimes;
    private String namespace;
    private String accessKey;
    private String secretKey;
    private boolean addMetadataKeyValuesAsProperties;

    @Override
    public JnksIotRocketMqNodeConfiguration defaultConfiguration() {
        JnksIotRocketMqNodeConfiguration configuration = new JnksIotRocketMqNodeConfiguration();
        configuration.setTopicPattern("my-topic");
        configuration.setNameServerAddress("localhost:9876");
        configuration.setProducerGroup("jnks-iot-rule-node-producer");
        configuration.setSendMode(JnksIotRocketMqNode.SendMode.ASYNC.name());
        configuration.setSendMsgTimeout(3000);
        configuration.setRetryTimes(2);
        configuration.setAddMetadataKeyValuesAsProperties(false);
        return configuration;
    }
}
