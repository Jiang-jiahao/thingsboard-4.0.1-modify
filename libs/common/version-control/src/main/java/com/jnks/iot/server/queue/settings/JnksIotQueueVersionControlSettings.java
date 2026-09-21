package com.jnks.iot.server.queue.settings;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Lazy
@Data
@Component
public class JnksIotQueueVersionControlSettings {

    @Value("${queue.vc.topic:jnks_iot_version_control}")
    private String topic;

    @Value("${queue.vc.usage-stats-topic:jnks_iot_usage_stats}")
    private String usageStatsTopic;

    @Value("${queue.vc.partitions:10}")
    private int partitions;
}
