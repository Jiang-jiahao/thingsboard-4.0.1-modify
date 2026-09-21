package com.jnks.iot.server.queue.provider;

import com.jnks.iot.server.gen.transport.TransportProtos.ToUsageStatsServiceMsg;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

/**
 * 使用统计
 */
public interface JnksIotUsageStatsClientQueueFactory {

    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgProducer();

}
