package com.jnks.iot.server.service.transport;

import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiRequestMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiResponseMsg;
import com.jnks.iot.server.queue.TbQueueHandler;
import com.jnks.iot.server.queue.common.TbProtoQueueMsg;

/**
 * Created by ashvayka on 05.10.18.
 */
public interface TransportApiService extends TbQueueHandler<TbProtoQueueMsg<TransportApiRequestMsg>, TbProtoQueueMsg<TransportApiResponseMsg>> {
}
