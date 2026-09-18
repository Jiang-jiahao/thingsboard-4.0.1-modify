package org.thingsboard.server.queue.provider;

import org.thingsboard.server.gen.transport.TransportProtos.FromEdqsMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToEdqsMsg;
import org.thingsboard.server.queue.TbQueueProducer;
import org.thingsboard.server.queue.TbQueueRequestTemplate;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;

/**
 * edqs客户端查询工厂
 */
public interface EdqsClientQueueFactory {

    TbQueueProducer<TbProtoQueueMsg<ToEdqsMsg>> createEdqsEventsProducer();

    TbQueueRequestTemplate<TbProtoQueueMsg<ToEdqsMsg>, TbProtoQueueMsg<FromEdqsMsg>> createEdqsRequestTemplate();

}
