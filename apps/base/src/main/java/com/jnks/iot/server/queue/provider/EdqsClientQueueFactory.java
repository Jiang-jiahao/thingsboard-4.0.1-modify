package com.jnks.iot.server.queue.provider;

import com.jnks.iot.server.gen.transport.TransportProtos.FromEdqsMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToEdqsMsg;
import com.jnks.iot.server.queue.TbQueueProducer;
import com.jnks.iot.server.queue.TbQueueRequestTemplate;
import com.jnks.iot.server.queue.common.TbProtoQueueMsg;

/**
 * edqs客户端查询工厂
 */
public interface EdqsClientQueueFactory {

    TbQueueProducer<TbProtoQueueMsg<ToEdqsMsg>> createEdqsEventsProducer();

    TbQueueRequestTemplate<TbProtoQueueMsg<ToEdqsMsg>, TbProtoQueueMsg<FromEdqsMsg>> createEdqsRequestTemplate();

}
