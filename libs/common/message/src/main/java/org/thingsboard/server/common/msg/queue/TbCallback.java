package org.thingsboard.server.common.msg.queue;

import org.thingsboard.server.common.data.id.EntityId;

import java.util.UUID;

public interface TbCallback {

    TbCallback EMPTY = new TbCallback() {

        @Override
        public void onSuccess() {

        }

        @Override
        public void onFailure(Throwable t) {

        }
    };

    default UUID getId(){
        return EntityId.NULL_UUID;
    }

    void onSuccess();

    void onFailure(Throwable t);

}
