package com.jnks.iot.server.queue;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.stats.MessagesStats;

public interface JnksIotQueueRequestTemplate<Request extends JnksIotQueueMsg, Response extends JnksIotQueueMsg> {

    void init();

    ListenableFuture<Response> send(Request request);

    ListenableFuture<Response> send(Request request, long timeoutNs);

    ListenableFuture<Response> send(Request request, Integer partition);

    void stop();

    void setMessagesStats(MessagesStats messagesStats);
}
