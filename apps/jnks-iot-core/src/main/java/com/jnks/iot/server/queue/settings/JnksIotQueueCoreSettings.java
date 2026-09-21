package com.jnks.iot.server.queue.settings;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Lazy
@Data
@Component
public class JnksIotQueueCoreSettings {

    @Value("${queue.core.topic}")
    private String topic;

    @Value("${queue.core.ota.topic:jnks_iot_ota_package}")
    private String otaPackageTopic;

    @Value("${queue.core.usage-stats-topic:jnks_iot_usage_stats}")
    private String usageStatsTopic;

    @Value("${queue.core.housekeeper.topic:jnks_iot_housekeeper}")
    private String housekeeperTopic;

    @Value("${queue.core.housekeeper.reprocessing-topic:jnks_iot_housekeeper.reprocessing}")
    private String housekeeperReprocessingTopic;

    @Value("${queue.core.partitions}")
    private int partitions;
}
