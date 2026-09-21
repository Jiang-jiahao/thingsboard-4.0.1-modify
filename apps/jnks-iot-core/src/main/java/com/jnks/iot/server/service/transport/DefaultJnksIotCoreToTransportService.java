package com.jnks.iot.server.service.transport;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportMsg;
import com.jnks.iot.server.queue.JnksIotQueueCallback;
import com.jnks.iot.server.queue.JnksIotQueueMsgMetadata;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.discovery.TopicService;
import com.jnks.iot.server.queue.provider.JnksIotQueueProducerProvider;
import java.util.UUID;
import java.util.function.Consumer;

import static com.jnks.iot.server.dao.model.ModelConstants.NULL_UUID;

/**
 * Core 向 Transport 节点推送通知。
 * <p>
 * 将会话相关消息（凭证更新、设备删除、属性下发等）发到指定 Transport 节点的通知 Topic。
 *
 * @see JnksIotCoreToTransportService
 */
@Slf4j
@Service
public class DefaultJnksIotCoreToTransportService implements JnksIotCoreToTransportService {

    private final TopicService topicService;
    private final JnksIotQueueProducer<JnksIotProtoQueueMsg<ToTransportMsg>> jnksIotTransportProducer;

    public DefaultJnksIotCoreToTransportService(TopicService topicService, JnksIotQueueProducerProvider jnksIotQueueProducerProvider) {
        this.topicService = topicService;
        this.jnksIotTransportProducer = jnksIotQueueProducerProvider.getTransportNotificationsMsgProducer();
    }

    /**
     * 向指定 Transport 节点推送消息（无回调）。
     */
    @Override
    public void process(String nodeId, ToTransportMsg msg) {
        process(nodeId, msg, null, null);
    }

    /**
     * 向指定 Transport 节点推送消息；{@code nodeId} 为空则跳过。
     */
    @Override
    public void process(String nodeId, ToTransportMsg msg, Runnable onSuccess, Consumer<Throwable> onFailure) {
        if (nodeId == null || nodeId.isEmpty()) {
            log.trace("process: skipping message without nodeId [{}], (ToTransportMsg) msg [{}]", nodeId, msg);
            if (onSuccess != null) {
                onSuccess.run();
            }
            return;
        }
        TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.JNKS_IOT_TRANSPORT, nodeId);
        UUID sessionId = new UUID(msg.getSessionIdMSB(), msg.getSessionIdLSB());
        log.trace("[{}][{}] Pushing session data to topic: {}", tpi.getFullTopicName(), sessionId, msg);
        JnksIotProtoQueueMsg<ToTransportMsg> queueMsg = new JnksIotProtoQueueMsg<>(NULL_UUID, msg);
        jnksIotTransportProducer.send(tpi, queueMsg, new QueueCallbackAdaptor(onSuccess, onFailure));
    }

    private static class QueueCallbackAdaptor implements JnksIotQueueCallback {
        private final Runnable onSuccess;
        private final Consumer<Throwable> onFailure;

        QueueCallbackAdaptor(Runnable onSuccess, Consumer<Throwable> onFailure) {
            this.onSuccess = onSuccess;
            this.onFailure = onFailure;
        }

        @Override
        public void onSuccess(JnksIotQueueMsgMetadata metadata) {
            if (onSuccess != null) {
                onSuccess.run();
            }
        }

        @Override
        public void onFailure(Throwable t) {
            if (onFailure != null) {
                onFailure.accept(t);
            }
        }
    }
}
