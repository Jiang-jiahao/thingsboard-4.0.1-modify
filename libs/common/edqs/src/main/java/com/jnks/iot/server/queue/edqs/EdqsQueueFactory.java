package com.jnks.iot.server.queue.edqs;

import com.jnks.iot.server.gen.transport.TransportProtos.FromEdqsMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToEdqsMsg;
import com.jnks.iot.server.queue.TbQueueAdmin;
import com.jnks.iot.server.queue.TbQueueConsumer;
import com.jnks.iot.server.queue.TbQueueProducer;
import com.jnks.iot.server.queue.TbQueueResponseTemplate;
import com.jnks.iot.server.queue.common.TbProtoQueueMsg;

public interface EdqsQueueFactory {

    TbQueueConsumer<TbProtoQueueMsg<ToEdqsMsg>> createEdqsEventsConsumer();

    TbQueueConsumer<TbProtoQueueMsg<ToEdqsMsg>> createEdqsEventsToBackupConsumer();

    TbQueueConsumer<TbProtoQueueMsg<ToEdqsMsg>> createEdqsStateConsumer();

    TbQueueProducer<TbProtoQueueMsg<ToEdqsMsg>> createEdqsStateProducer();

    TbQueueResponseTemplate<TbProtoQueueMsg<ToEdqsMsg>, TbProtoQueueMsg<FromEdqsMsg>> createEdqsResponseTemplate();

    TbQueueAdmin getEdqsQueueAdmin();

}
