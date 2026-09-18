package org.thingsboard.server.service.transport;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.gen.transport.TransportProtos.ToTransportMsg;
import org.thingsboard.server.queue.TbQueueCallback;
import org.thingsboard.server.queue.TbQueueMsgMetadata;
import org.thingsboard.server.queue.TbQueueProducer;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.discovery.TopicService;
import org.thingsboard.server.queue.provider.TbQueueProducerProvider;
import java.util.UUID;
import java.util.function.Consumer;

import static org.thingsboard.server.dao.model.ModelConstants.NULL_UUID;

/**
 * Core 向 Transport 节点推送通知。
 * <p>
 * 将会话相关消息（凭证更新、设备删除、属性下发等）发到指定 Transport 节点的通知 Topic。
 *
 * @see TbCoreToTransportService
 */
@Slf4j
@Service
public class DefaultTbCoreToTransportService implements TbCoreToTransportService {

    private final TopicService topicService;
    private final TbQueueProducer<TbProtoQueueMsg<ToTransportMsg>> tbTransportProducer;

    public DefaultTbCoreToTransportService(TopicService topicService, TbQueueProducerProvider tbQueueProducerProvider) {
        this.topicService = topicService;
        this.tbTransportProducer = tbQueueProducerProvider.getTransportNotificationsMsgProducer();
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
        TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.TB_TRANSPORT, nodeId);
        UUID sessionId = new UUID(msg.getSessionIdMSB(), msg.getSessionIdLSB());
        log.trace("[{}][{}] Pushing session data to topic: {}", tpi.getFullTopicName(), sessionId, msg);
        TbProtoQueueMsg<ToTransportMsg> queueMsg = new TbProtoQueueMsg<>(NULL_UUID, msg);
        tbTransportProducer.send(tpi, queueMsg, new QueueCallbackAdaptor(onSuccess, onFailure));
    }

    private static class QueueCallbackAdaptor implements TbQueueCallback {
        private final Runnable onSuccess;
        private final Consumer<Throwable> onFailure;

        QueueCallbackAdaptor(Runnable onSuccess, Consumer<Throwable> onFailure) {
            this.onSuccess = onSuccess;
            this.onFailure = onFailure;
        }

        @Override
        public void onSuccess(TbQueueMsgMetadata metadata) {
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
