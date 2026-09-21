package com.jnks.iot.rule.engine.aws.sns;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

@Data
public class JnksIotSnsNodeConfiguration implements NodeConfiguration<JnksIotSnsNodeConfiguration> {

    private String topicArnPattern;
    private String accessKeyId;
    private String secretAccessKey;
    private String region;

    @Override
    public JnksIotSnsNodeConfiguration defaultConfiguration() {
        JnksIotSnsNodeConfiguration configuration = new JnksIotSnsNodeConfiguration();
        configuration.setTopicArnPattern("arn:aws:sns:us-east-1:123456789012:MyNewTopic");
        configuration.setRegion("us-east-1");
        return configuration;
    }
}
