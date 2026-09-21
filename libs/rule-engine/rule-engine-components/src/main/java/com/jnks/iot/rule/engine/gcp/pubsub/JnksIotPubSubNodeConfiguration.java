package com.jnks.iot.rule.engine.gcp.pubsub;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

import java.util.Collections;
import java.util.Map;

@Data
public class JnksIotPubSubNodeConfiguration implements NodeConfiguration<JnksIotPubSubNodeConfiguration> {

    private String projectId;
    private String topicName;
    private Map<String, String> messageAttributes;
    private String serviceAccountKey;
    private String serviceAccountKeyFileName;

    @Override
    public JnksIotPubSubNodeConfiguration defaultConfiguration() {
        JnksIotPubSubNodeConfiguration configuration = new JnksIotPubSubNodeConfiguration();
        configuration.setProjectId("my-google-cloud-project-id");
        configuration.setTopicName("my-pubsub-topic-name");
        configuration.setMessageAttributes(Collections.emptyMap());
        return configuration;
    }
}
