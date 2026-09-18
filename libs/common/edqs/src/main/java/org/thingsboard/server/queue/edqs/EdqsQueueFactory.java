package org.thingsboard.server.queue.edqs;

import org.thingsboard.server.gen.transport.TransportProtos.FromEdqsMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToEdqsMsg;
import org.thingsboard.server.queue.TbQueueAdmin;
import org.thingsboard.server.queue.TbQueueConsumer;
import org.thingsboard.server.queue.TbQueueProducer;
import org.thingsboard.server.queue.TbQueueResponseTemplate;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;

public interface EdqsQueueFactory {

    TbQueueConsumer<TbProtoQueueMsg<ToEdqsMsg>> createEdqsEventsConsumer();

    TbQueueConsumer<TbProtoQueueMsg<ToEdqsMsg>> createEdqsEventsToBackupConsumer();

    TbQueueConsumer<TbProtoQueueMsg<ToEdqsMsg>> createEdqsStateConsumer();

    TbQueueProducer<TbProtoQueueMsg<ToEdqsMsg>> createEdqsStateProducer();

    TbQueueResponseTemplate<TbProtoQueueMsg<ToEdqsMsg>, TbProtoQueueMsg<FromEdqsMsg>> createEdqsResponseTemplate();

    TbQueueAdmin getEdqsQueueAdmin();

}
