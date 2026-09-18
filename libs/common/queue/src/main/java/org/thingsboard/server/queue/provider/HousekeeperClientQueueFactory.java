package org.thingsboard.server.queue.provider;

import org.thingsboard.server.gen.transport.TransportProtos.ToHousekeeperServiceMsg;
import org.thingsboard.server.queue.TbQueueProducer;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;

/**
 * 后台维护
 */
public interface HousekeeperClientQueueFactory {

    TbQueueProducer<TbProtoQueueMsg<ToHousekeeperServiceMsg>> createHousekeeperMsgProducer();

}
