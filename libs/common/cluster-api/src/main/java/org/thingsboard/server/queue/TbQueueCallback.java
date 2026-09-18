package org.thingsboard.server.queue;


public interface TbQueueCallback {

    TbQueueCallback EMPTY = new TbQueueCallback() {

        @Override
        public void onSuccess(TbQueueMsgMetadata metadata) {

        }

        @Override
        public void onFailure(Throwable t) {

        }
    };

    void onSuccess(TbQueueMsgMetadata metadata);

    void onFailure(Throwable t);
}
