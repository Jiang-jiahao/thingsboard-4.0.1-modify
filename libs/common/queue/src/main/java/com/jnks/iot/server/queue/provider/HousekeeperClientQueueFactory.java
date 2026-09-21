package com.jnks.iot.server.queue.provider;

import com.jnks.iot.server.gen.transport.TransportProtos.ToHousekeeperServiceMsg;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

/**
 * 后台维护
 */
public interface HousekeeperClientQueueFactory {

    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> createHousekeeperMsgProducer();

}
