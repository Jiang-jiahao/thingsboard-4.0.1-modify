package com.jnks.iot.server.queue.provider;

import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiRequestMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiResponseMsg;
import com.jnks.iot.server.queue.JnksIotQueueConsumer;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.JnksIotQueueRequestTemplate;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

public interface JnksIotTransportQueueFactory extends JnksIotUsageStatsClientQueueFactory, HousekeeperClientQueueFactory {

    JnksIotQueueRequestTemplate<JnksIotProtoQueueMsg<TransportApiRequestMsg>, JnksIotProtoQueueMsg<TransportApiResponseMsg>> createTransportApiRequestTemplate();

    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> createRuleEngineMsgProducer();

    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreMsg>> createJnksIotCoreMsgProducer();

    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> createJnksIotCoreNotificationsMsgProducer();

    JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToTransportMsg>> createTransportNotificationsConsumer();

}
