package com.jnks.iot.server.common.data.queue;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import com.jnks.iot.server.common.data.BaseDataWithAdditionalInfo;
import com.jnks.iot.server.common.data.HasName;
import com.jnks.iot.server.common.data.HasTenantId;
import com.jnks.iot.server.common.data.id.QueueId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.tenant.profile.TenantProfileQueueConfiguration;
import com.jnks.iot.server.common.data.validation.Length;
import com.jnks.iot.server.common.data.validation.NoXss;

import java.util.Optional;

@Data
public class Queue extends BaseDataWithAdditionalInfo<QueueId> implements HasName, HasTenantId, QueueConfig {
    private TenantId tenantId;
    @NoXss
    @Length(fieldName = "name")
    private String name;
    @NoXss
    @Length(fieldName = "topic")
    private String topic;
    private int pollInterval;
    private int partitions;
    private boolean consumerPerPartition;
    private long packProcessingTimeout;
    private SubmitStrategy submitStrategy;
    private ProcessingStrategy processingStrategy;

    public Queue() {
    }

    public Queue(QueueId id) {
        super(id);
    }

    public Queue(TenantId tenantId, TenantProfileQueueConfiguration queueConfiguration) {
        this.tenantId = tenantId;
        this.name = queueConfiguration.getName();
        this.topic = queueConfiguration.getTopic();
        this.pollInterval = queueConfiguration.getPollInterval();
        this.partitions = queueConfiguration.getPartitions();
        this.consumerPerPartition = queueConfiguration.isConsumerPerPartition();
        this.packProcessingTimeout = queueConfiguration.getPackProcessingTimeout();
        this.submitStrategy = queueConfiguration.getSubmitStrategy();
        this.processingStrategy = queueConfiguration.getProcessingStrategy();
        setAdditionalInfo(queueConfiguration.getAdditionalInfo());
    }


    @JsonIgnore
    public String getCustomProperties() {
        return Optional.ofNullable(getAdditionalInfo())
                .map(info -> info.get("customProperties"))
                .filter(JsonNode::isTextual).map(JsonNode::asText).orElse(null);
    }

    @JsonIgnore
    public boolean isDuplicateMsgToAllPartitions() {
        return Optional.ofNullable(getAdditionalInfo())
                .map(info -> info.get("duplicateMsgToAllPartitions"))
                .filter(JsonNode::isBoolean).map(JsonNode::asBoolean).orElse(false);
    }

}
