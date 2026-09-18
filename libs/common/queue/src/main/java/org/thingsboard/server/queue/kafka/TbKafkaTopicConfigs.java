package org.thingsboard.server.queue.kafka;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.thingsboard.server.queue.util.PropertyUtils;

import java.util.Map;

@Component
@ConditionalOnProperty(prefix = "queue", value = "type", havingValue = "kafka")
public class TbKafkaTopicConfigs {

    public static final String NUM_PARTITIONS_SETTING = "partitions";

    @Value("${queue.kafka.topic-properties.core:}")
    private String coreProperties;
    @Value("${queue.kafka.topic-properties.rule-engine:}")
    private String ruleEngineProperties;
    @Value("${queue.kafka.topic-properties.transport-api:}")
    private String transportApiProperties;
    @Value("${queue.kafka.topic-properties.notifications:}")
    private String notificationsProperties;
    @Value("${queue.kafka.topic-properties.js-executor:}")
    private String jsExecutorProperties;
    @Value("${queue.kafka.topic-properties.ota-updates:}")
    private String fwUpdatesProperties;
    @Value("${queue.kafka.topic-properties.version-control:}")
    private String vcProperties;
    @Value("${queue.kafka.topic-properties.housekeeper:}")
    private String housekeeperProperties;
    @Value("${queue.kafka.topic-properties.housekeeper-reprocessing:}")
    private String housekeeperReprocessingProperties;
    @Value("${queue.kafka.topic-properties.calculated-field:}")
    private String calculatedFieldProperties;
    @Value("${queue.kafka.topic-properties.calculated-field-state:}")
    private String calculatedFieldStateProperties;
    @Value("${queue.kafka.topic-properties.edqs-events:}")
    private String edqsEventsProperties;
    @Value("${queue.kafka.topic-properties.edqs-requests:}")
    private String edqsRequestsProperties;
    @Value("${queue.kafka.topic-properties.edqs-state:}")
    private String edqsStateProperties;

    @Getter
    private Map<String, String> coreConfigs;
    @Getter
    private Map<String, String> ruleEngineConfigs;
    @Getter
    private Map<String, String> transportApiRequestConfigs;
    @Getter
    private Map<String, String> transportApiResponseConfigs;
    @Getter
    private Map<String, String> notificationsConfigs;
    @Getter
    private Map<String, String> jsExecutorRequestConfigs;
    @Getter
    private Map<String, String> jsExecutorResponseConfigs;
    @Getter
    private Map<String, String> fwUpdatesConfigs;
    @Getter
    private Map<String, String> vcConfigs;
    @Getter
    private Map<String, String> housekeeperConfigs;
    @Getter
    private Map<String, String> housekeeperReprocessingConfigs;
    @Getter
    private Map<String, String> calculatedFieldConfigs;
    @Getter
    private Map<String, String> calculatedFieldStateConfigs;
    @Getter
    private Map<String, String> edqsEventsConfigs;
    @Getter
    private Map<String, String> edqsRequestsConfigs;
    @Getter
    private Map<String, String> edqsStateConfigs;

    @PostConstruct
    private void init() {
        coreConfigs = PropertyUtils.getProps(coreProperties);
        ruleEngineConfigs = PropertyUtils.getProps(ruleEngineProperties);
        transportApiRequestConfigs = PropertyUtils.getProps(transportApiProperties);
        transportApiResponseConfigs = PropertyUtils.getProps(transportApiProperties);
        transportApiResponseConfigs.put(NUM_PARTITIONS_SETTING, "1");
        notificationsConfigs = PropertyUtils.getProps(notificationsProperties);
        jsExecutorRequestConfigs = PropertyUtils.getProps(jsExecutorProperties);
        jsExecutorResponseConfigs = PropertyUtils.getProps(jsExecutorProperties);
        jsExecutorResponseConfigs.put(NUM_PARTITIONS_SETTING, "1");
        fwUpdatesConfigs = PropertyUtils.getProps(fwUpdatesProperties);
        vcConfigs = PropertyUtils.getProps(vcProperties);
        housekeeperConfigs = PropertyUtils.getProps(housekeeperProperties);
        housekeeperReprocessingConfigs = PropertyUtils.getProps(housekeeperReprocessingProperties);
        calculatedFieldConfigs = PropertyUtils.getProps(calculatedFieldProperties);
        calculatedFieldStateConfigs = PropertyUtils.getProps(calculatedFieldStateProperties);
        edqsEventsConfigs = PropertyUtils.getProps(edqsEventsProperties);
        edqsRequestsConfigs = PropertyUtils.getProps(edqsRequestsProperties);
        edqsStateConfigs = PropertyUtils.getProps(edqsStateProperties);
    }

}
