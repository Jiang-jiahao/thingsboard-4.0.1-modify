package com.jnks.iot.server.queue;


public interface JnksIotQueueCallback {

    JnksIotQueueCallback EMPTY = new JnksIotQueueCallback() {

        @Override
        public void onSuccess(JnksIotQueueMsgMetadata metadata) {

        }

        @Override
        public void onFailure(Throwable t) {

        }
    };

    void onSuccess(JnksIotQueueMsgMetadata metadata);

    void onFailure(Throwable t);
}
