package com.jnks.iot.server.queue.provider;

import com.jnks.iot.server.gen.transport.TransportProtos.ToUsageStatsServiceMsg;
import com.jnks.iot.server.queue.TbQueueProducer;
import com.jnks.iot.server.queue.common.TbProtoQueueMsg;

/**
 * 使用统计
 */
public interface TbUsageStatsClientQueueFactory {

    TbQueueProducer<TbProtoQueueMsg<ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgProducer();

}
