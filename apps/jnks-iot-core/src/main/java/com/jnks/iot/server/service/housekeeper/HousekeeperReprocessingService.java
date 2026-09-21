package com.jnks.iot.server.service.housekeeper;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.JnksIotThreadFactory;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.gen.transport.TransportProtos.HousekeeperTaskProto;
import com.jnks.iot.server.gen.transport.TransportProtos.ToHousekeeperServiceMsg;
import com.jnks.iot.server.queue.JnksIotQueueConsumer;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.common.consumer.QueueConsumerManager;
import com.jnks.iot.server.common.housekeeper.HousekeeperConfig;
import com.jnks.iot.server.queue.provider.JnksIotCoreQueueFactory;
import com.jnks.iot.common.util.AfterStartUp;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Housekeeper 失败任务重处理服务：将处理失败的任务延迟后再投递并消费。
 * <p>
 * <b>职责：</b>把失败任务写入重处理 Topic（单分区，集群内仅一个 Core 消费），
 * 延迟后再次调用 {@link HousekeeperService#processTask}。
 * <p>
 * <b>触发方式：</b>启动后订阅重处理队列；失败时由主服务调用 {@link #submitForReprocessing}。
 * <p>
 * <b>清理对象：</b>与主服务相同的 Housekeeper 任务。
 */
@Service
@Slf4j
public class HousekeeperReprocessingService {

    private final HousekeeperConfig config;
    private final HousekeeperService housekeeperService;
    private final QueueConsumerManager<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> consumer;
    private final JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> producer;
    private final TopicPartitionInfo submitTpi;

    private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor(JnksIotThreadFactory.forName("housekeeper-reprocessing-consumer"));

    public HousekeeperReprocessingService(HousekeeperConfig config,
                                          @Lazy HousekeeperService housekeeperService,
                                          JnksIotCoreQueueFactory queueFactory) {
        this.config = config;
        this.housekeeperService = housekeeperService;
        this.consumer = QueueConsumerManager.<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>>builder()
                .name("Housekeeper reprocessing")
                .msgPackProcessor(this::processMsgs)
                .pollInterval(config.getPollInterval())
                .consumerCreator(queueFactory::createHousekeeperReprocessingMsgConsumer)
                .consumerExecutor(consumerExecutor)
                .build();
        this.producer = queueFactory.createHousekeeperReprocessingMsgProducer();
        this.submitTpi = TopicPartitionInfo.builder().topic(producer.getDefaultTopic()).build();
    }

    /** 启动后订阅并拉起重处理队列消费者。 */
    @AfterStartUp(order = AfterStartUp.REGULAR_SERVICE)
    public void afterStartUp() {
        consumer.subscribe(); // Kafka topic for tasks reprocessing has only 1 partition, so only one TB Core will reprocess tasks
        consumer.launch();
    }

    private void processMsgs(List<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> msgs, JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> consumer) throws Exception {
        Thread.sleep(config.getTaskReprocessingDelay());

        for (JnksIotProtoQueueMsg<ToHousekeeperServiceMsg> msg : msgs) {
            log.trace("Reprocessing task: {}", msg);
            try {
                housekeeperService.processTask(msg.getValue());
            } catch (InterruptedException e) {
                return;
            } catch (Throwable e) {
                log.error("Unexpected error during message reprocessing [{}]", msg, e);
                submitForReprocessing(msg.getValue(), e);
            }
        }
        consumer.commit();
    }

    /** 将失败任务递增尝试次数并附带错误栈后重新投递。 */
    public void submitForReprocessing(ToHousekeeperServiceMsg msg, Throwable error) {
        HousekeeperTaskProto task = msg.getTask();
        Set<String> errors = new LinkedHashSet<>(task.getErrorsList());
        errors.add(StringUtils.truncate(ExceptionUtils.getStackTrace(error), 1024));
        msg = msg.toBuilder()
                .setTask(task.toBuilder()
                        .setAttempt(task.getAttempt() + 1)
                        .clearErrors().addAllErrors(errors)
                        .build())
                .build();

        log.trace("Submitting for reprocessing: {}", msg);
        producer.send(submitTpi, new JnksIotProtoQueueMsg<>(UUID.randomUUID(), msg), null);
    }

    @PreDestroy
    private void stop() {
        consumer.stop();
        consumerExecutor.shutdownNow();
        log.info("Stopped Housekeeper reprocessing service");
    }

}
