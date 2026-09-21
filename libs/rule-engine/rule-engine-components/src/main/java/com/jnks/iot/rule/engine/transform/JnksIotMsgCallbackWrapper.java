package com.jnks.iot.rule.engine.transform;

public interface JnksIotMsgCallbackWrapper {

    void onSuccess();

    void onFailure(Throwable t);
}
