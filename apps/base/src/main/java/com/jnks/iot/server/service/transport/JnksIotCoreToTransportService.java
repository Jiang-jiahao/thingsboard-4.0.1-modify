package com.jnks.iot.server.service.transport;

import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportMsg;

import java.util.function.Consumer;

public interface JnksIotCoreToTransportService {

    void process(String nodeId, ToTransportMsg msg);

    void process(String nodeId, ToTransportMsg msg, Runnable onSuccess, Consumer<Throwable> onFailure);

}
