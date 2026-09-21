package com.jnks.iot.server.queue.provider;

import com.jnks.iot.server.gen.js.JsInvokeProtos;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToHousekeeperServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToOtaPackageStateServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToUsageStatsServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToVersionControlServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiRequestMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiResponseMsg;
import com.jnks.iot.server.queue.JnksIotQueueConsumer;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.JnksIotQueueRequestTemplate;
import com.jnks.iot.server.queue.common.JnksIotProtoJsQueueMsg;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

/**
 * 负责初始化TB核心模块使用的各种生产者和消费者。实现依赖于队列queue。队列的类型可以在yml或JNKS_IOT_QUEUE_TYPE环境变量配置
 */
public interface JnksIotCoreQueueFactory extends JnksIotUsageStatsClientQueueFactory, HousekeeperClientQueueFactory, EdqsClientQueueFactory {

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
     * Used to consume messages by TB Core Service
     *
     * @return
     */
    JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToCoreMsg>> createToCoreMsgConsumer();

    /**
     * Used to consume messages about usage statistics by TB Core Service
     *
     * @return
     */
    JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgConsumer();

    /**
     * Used to consume messages about firmware update notifications by TB Core Service
     *
     * @return
     */
    JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToOtaPackageStateServiceMsg>> createToOtaPackageStateServiceMsgConsumer();

    /**
     * Used to consume messages about firmware update notifications by TB Core Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToOtaPackageStateServiceMsg>> createToOtaPackageStateServiceMsgProducer();

    /**
     * Used to consume high priority messages by TB Core Service
     *
     * @return
     */
    JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> createToCoreNotificationsMsgConsumer();

    /**
     * Used to consume Transport API Calls
     *
     * @return
     */
    JnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportApiRequestMsg>> createTransportApiRequestConsumer();

    /**
     * Used to push replies to Transport API Calls
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportApiResponseMsg>> createTransportApiResponseProducer();

    JnksIotQueueRequestTemplate<JnksIotProtoJsQueueMsg<JsInvokeProtos.RemoteJsRequest>, JnksIotProtoQueueMsg<JsInvokeProtos.RemoteJsResponse>> createRemoteJsRequestTemplate();

    /**
     * Used to push messages to instances of TB Version Control Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToVersionControlServiceMsg>> createVersionControlMsgProducer();

    JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> createHousekeeperMsgConsumer();

    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> createHousekeeperReprocessingMsgProducer();

    JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> createHousekeeperReprocessingMsgConsumer();

    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldMsg>> createToCalculatedFieldMsgProducer();

    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> createToCalculatedFieldNotificationMsgProducer();

}
