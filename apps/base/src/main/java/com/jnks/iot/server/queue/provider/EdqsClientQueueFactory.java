package com.jnks.iot.server.queue.provider;

import com.jnks.iot.server.gen.transport.TransportProtos.FromEdqsMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToEdqsMsg;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.JnksIotQueueRequestTemplate;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

/**
 * edqs客户端查询工厂
 */
public interface EdqsClientQueueFactory {

    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsEventsProducer();

    JnksIotQueueRequestTemplate<JnksIotProtoQueueMsg<ToEdqsMsg>, JnksIotProtoQueueMsg<FromEdqsMsg>> createEdqsRequestTemplate();

}
