package com.jnks.iot.script.api.tbel;

import com.google.common.util.concurrent.ListenableFuture;
import org.mvel2.ExecutionContext;
import com.jnks.iot.script.api.JnksIotScriptExecutionTask;


public class TbelScriptExecutionTask extends JnksIotScriptExecutionTask {

    private final ExecutionContext context;

    public TbelScriptExecutionTask(ExecutionContext context, ListenableFuture<Object> resultFuture) {
        super(resultFuture);
        this.context = context;
    }

    @Override
    public void stop(){
        context.stop();
    }
}
