package com.jnks.iot.server.queue.provider;

import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToHousekeeperServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToUsageStatsServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToVersionControlServiceMsg;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

/**
 * 负责为其他服务提供各种生产者
 */
public interface JnksIotQueueProducerProvider {

    /**
     * Used to push messages to instances of TB Transport Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToTransportMsg>> getTransportNotificationsMsgProducer();

    /**
     * Used to push messages to instances of TB RuleEngine Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> getRuleEngineMsgProducer();

    /**
     * Used to push notifications to instances of TB RuleEngine Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg>> getRuleEngineNotificationsMsgProducer();

    /**
     * Used to push messages to other instances of TB Core Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreMsg>> getJnksIotCoreMsgProducer();

    /**
     * Used to push messages to other instances of TB Core Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> getJnksIotCoreNotificationsMsgProducer();

    /**
     * Used to push messages to other instances of TB Core Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> getJnksIotUsageStatsMsgProducer();

    /**
     * Used to push messages to other instances of TB Core Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToVersionControlServiceMsg>> getJnksIotVersionControlMsgProducer();

    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> getHousekeeperMsgProducer();

    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldMsg>> getCalculatedFieldsMsgProducer();

    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> getCalculatedFieldsNotificationsMsgProducer();

}
