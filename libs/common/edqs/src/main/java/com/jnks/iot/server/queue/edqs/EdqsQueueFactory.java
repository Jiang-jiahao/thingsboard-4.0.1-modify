package com.jnks.iot.server.queue.edqs;

import com.jnks.iot.server.gen.transport.TransportProtos.FromEdqsMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToEdqsMsg;
import com.jnks.iot.server.queue.JnksIotQueueAdmin;
import com.jnks.iot.server.queue.JnksIotQueueConsumer;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.JnksIotQueueResponseTemplate;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

public interface EdqsQueueFactory {

    JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsEventsConsumer();

    JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsEventsToBackupConsumer();

    JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsStateConsumer();

    JnksIotQueueProducer<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsStateProducer();

    JnksIotQueueResponseTemplate<JnksIotProtoQueueMsg<ToEdqsMsg>, JnksIotProtoQueueMsg<FromEdqsMsg>> createEdqsResponseTemplate();

    JnksIotQueueAdmin getEdqsQueueAdmin();

}
