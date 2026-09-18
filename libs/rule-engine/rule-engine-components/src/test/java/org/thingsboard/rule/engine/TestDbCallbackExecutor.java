package org.thingsboard.rule.engine;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.thingsboard.common.util.ListeningExecutor;

import java.util.concurrent.Callable;

public class TestDbCallbackExecutor implements ListeningExecutor {

    @Override
    public <T> ListenableFuture<T> executeAsync(Callable<T> task) {
        try {
            return Futures.immediateFuture(task.call());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Runnable command) {
        command.run();
    }

}
