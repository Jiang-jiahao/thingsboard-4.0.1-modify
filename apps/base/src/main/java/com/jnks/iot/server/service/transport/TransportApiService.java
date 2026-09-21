package com.jnks.iot.server.service.transport;

import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiRequestMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiResponseMsg;
import com.jnks.iot.server.queue.JnksIotQueueHandler;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

/**
 * Created by ashvayka on 05.10.18.
 */
public interface TransportApiService extends JnksIotQueueHandler<JnksIotProtoQueueMsg<TransportApiRequestMsg>, JnksIotProtoQueueMsg<TransportApiResponseMsg>> {
}
