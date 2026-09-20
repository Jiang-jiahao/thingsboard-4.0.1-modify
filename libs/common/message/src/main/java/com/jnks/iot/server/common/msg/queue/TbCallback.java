package com.jnks.iot.server.common.msg.queue;

import com.jnks.iot.server.common.data.id.EntityId;

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
