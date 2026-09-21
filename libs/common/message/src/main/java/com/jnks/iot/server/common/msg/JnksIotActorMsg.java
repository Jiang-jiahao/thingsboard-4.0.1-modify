package com.jnks.iot.server.common.msg;

/**
 * Created by ashvayka on 15.03.18.
 */
public interface JnksIotActorMsg {

    MsgType getMsgType();

    /**
     * Executed when the target JnksIotActor is stopped or destroyed.
     * For example, rule node failed to initialize or removed from rule chain.
     * Implementation should cleanup the resources.
     */
    default void onJnksIotActorStopped(JnksIotActorStopReason reason) {
    }

}
