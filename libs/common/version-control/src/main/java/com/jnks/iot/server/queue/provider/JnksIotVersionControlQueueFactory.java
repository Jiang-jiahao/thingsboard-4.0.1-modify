package com.jnks.iot.server.queue.provider;

import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToVersionControlServiceMsg;
import com.jnks.iot.server.queue.JnksIotQueueConsumer;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

/**
 * Responsible for initialization of various Producers and Consumers used by TB Version Control Node.
 * Implementation Depends on the queue queue.type from yml or JNKS_IOT_QUEUE_TYPE environment variable
 */
public interface JnksIotVersionControlQueueFactory extends JnksIotUsageStatsClientQueueFactory, HousekeeperClientQueueFactory {

    /**
     * Used to push notifications to other instances of TB Core Service
     *
     * @return
     */
    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> createJnksIotCoreNotificationsMsgProducer();

    /**
     * Used to consume messages from TB Core Service
     *
     * @return
     */
    JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToVersionControlServiceMsg>> createToVersionControlMsgConsumer();

}
