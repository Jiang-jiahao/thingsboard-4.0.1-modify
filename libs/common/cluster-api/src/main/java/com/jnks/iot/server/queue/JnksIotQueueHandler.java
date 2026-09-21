package com.jnks.iot.server.queue;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Created by ashvayka on 05.10.18.
 */
public interface JnksIotQueueHandler<Request extends JnksIotQueueMsg, Response extends JnksIotQueueMsg> {

    ListenableFuture<Response> handle(Request request);

}
