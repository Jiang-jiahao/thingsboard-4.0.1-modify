package com.jnks.iot.script.api.js;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.script.api.JnksIotScriptExecutionTask;

public class JsScriptExecutionTask extends JnksIotScriptExecutionTask {

    public JsScriptExecutionTask(ListenableFuture<Object> resultFuture) {
        super(resultFuture);
    }

    @Override
    public void stop() {
        // do nothing
    }
}
