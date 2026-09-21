package com.jnks.iot.server.queue.provider;

import com.jnks.iot.server.common.data.queue.Queue;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.gen.js.JsInvokeProtos;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldStateProto;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToOtaPackageStateServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportMsg;
import com.jnks.iot.server.queue.JnksIotQueueAdmin;
import com.jnks.iot.server.queue.JnksIotQueueConsumer;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.JnksIotQueueRequestTemplate;
import com.jnks.iot.server.queue.common.JnksIotProtoJsQueueMsg;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

/**
 * Responsible for initialization of various Producers and Consumers used by TB Core Node.
 * Implementation Depends on the queue queue.type from yml or JNKS_IOT_QUEUE_TYPE environment variable
 */
public interface JnksIotRuleEngineQueueFactory extends JnksIotUsageStatsClientQueueFactory, HousekeeperClientQueueFactory, EdqsClientQueueFactory {

    /**
     * Used to push messages to instances of TB Transport Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToTransportMsg>> createTransportNotificationsMsgProducer();

    /**
     * Used to push messages to instances of TB RuleEngine Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> createRuleEngineMsgProducer();

    /**
     * Used to push notifications to instances of TB RuleEngine Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg>> createRuleEngineNotificationsMsgProducer();

    /**
     * Used to push messages to other instances of TB Core Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreMsg>> createJnksIotCoreMsgProducer();

    /**
     * Used to push notifications to other instances of TB Core Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> createJnksIotCoreNotificationsMsgProducer();

    /**
     * Used to consume messages about firmware update notifications to TB Core Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToOtaPackageStateServiceMsg>> createToOtaPackageStateServiceMsgProducer();

    /**
     * Used to consume messages by TB Rule Engine Service
     *
     * @param configuration
     * @return
     */
    JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> createToRuleEngineMsgConsumer(Queue configuration);

    /**
     * Used to consume messages by TB Rule Engine Service
     * Intended usage for consumer per partition strategy
     *
     * @param configuration
     * @param partitionId   as a suffix for consumer name
     * @return JnksIotQueueConsumer
     */
    default JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> createToRuleEngineMsgConsumer(Queue configuration, Integer partitionId) {
        return createToRuleEngineMsgConsumer(configuration);
    }

    /**
     * Used to consume high priority messages by TB Rule Engine Service
     *
     * @return
     */
    JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg>> createToRuleEngineNotificationsMsgConsumer();

    JnksIotQueueRequestTemplate<JnksIotProtoJsQueueMsg<JsInvokeProtos.RemoteJsRequest>, JnksIotProtoQueueMsg<JsInvokeProtos.RemoteJsResponse>> createRemoteJsRequestTemplate();

    JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToCalculatedFieldMsg>> createToCalculatedFieldMsgConsumer(TopicPartitionInfo tpi);

    JnksIotQueueAdmin getCalculatedFieldQueueAdmin();

    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldMsg>> createToCalculatedFieldMsgProducer();

    JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> createToCalculatedFieldNotificationMsgConsumer();

    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> createToCalculatedFieldNotificationMsgProducer();

    JnksIotQueueConsumer<JnksIotProtoQueueMsg<CalculatedFieldStateProto>> createCalculatedFieldStateConsumer();

    JnksIotQueueProducer<JnksIotProtoQueueMsg<CalculatedFieldStateProto>> createCalculatedFieldStateProducer();

}
