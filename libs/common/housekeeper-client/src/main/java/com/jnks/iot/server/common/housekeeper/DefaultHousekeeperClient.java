package com.jnks.iot.server.common.housekeeper;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.housekeeper.HousekeeperTask;
import com.jnks.iot.server.common.data.housekeeper.HousekeeperTaskType;
import com.jnks.iot.server.common.msg.housekeeper.HousekeeperClient;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.gen.transport.TransportProtos.ToHousekeeperServiceMsg;
import com.jnks.iot.server.queue.JnksIotQueueCallback;
import com.jnks.iot.server.queue.JnksIotQueueMsgMetadata;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.provider.JnksIotQueueProducerProvider;

@Service
@Slf4j
public class DefaultHousekeeperClient implements HousekeeperClient {

    private final HousekeeperConfig config;
    private final JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> producer;
    private final TopicPartitionInfo submitTpi;
    private final JnksIotQueueCallback submitCallback;

    public DefaultHousekeeperClient(HousekeeperConfig config,
                                    JnksIotQueueProducerProvider producerProvider) {
        this.config = config;
        this.producer = producerProvider.getHousekeeperMsgProducer();
        this.submitTpi = TopicPartitionInfo.builder().topic(producer.getDefaultTopic()).build();
        this.submitCallback = new JnksIotQueueCallback() {
            @Override
            public void onSuccess(JnksIotQueueMsgMetadata metadata) {
                log.trace("Submitted Housekeeper task");
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Failed to submit Housekeeper task", t);
            }
        };
    }

    @Override
    public void submitTask(HousekeeperTask task) {
        HousekeeperTaskType taskType = task.getTaskType();
        if (config.getDisabledTaskTypes().contains(taskType)) {
            log.trace("Task type {} is disabled, ignoring {}", taskType, task);
            return;
        }

        log.debug("[{}][{}][{}] Submitting task: {}", task.getTenantId(), task.getEntityId().getEntityType(), task.getEntityId(), task);
        /*
         * using msg key as entity id so that msgs related to certain entity are pushed to same partition,
         * e.g. on tenant deletion (entity id is tenant id), we need to clean up tenant entities in certain order
         * */
        try {
            producer.send(submitTpi, new JnksIotProtoQueueMsg<>(task.getEntityId().getId(), ToHousekeeperServiceMsg.newBuilder()
                    .setTask(TransportProtos.HousekeeperTaskProto.newBuilder()
                            .setValue(JacksonUtil.toString(task))
                            .setTs(task.getTs())
                            .setAttempt(0)
                            .build())
                    .build()), submitCallback);
        } catch (Throwable t) {
            log.error("Failed to submit Housekeeper task {}", task, t);
        }
    }

}
