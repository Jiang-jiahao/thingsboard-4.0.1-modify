package com.jnks.iot.server.dao.nosql;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import com.jnks.iot.common.util.JnksIotExecutors;

import java.util.concurrent.ExecutorService;

/**
 * Created by ashvayka on 21.02.17.
 */
public abstract class CassandraAbstractAsyncDao extends CassandraAbstractDao {

    protected ExecutorService readResultsProcessingExecutor;

    @Value("${cassandra.query.result_processing_threads:50}")
    private int threadPoolSize;

    @PostConstruct
    public void startExecutor() {
        readResultsProcessingExecutor = JnksIotExecutors.newWorkStealingPool(threadPoolSize, "cassandra-callback");
    }

    @PreDestroy
    public void stopExecutor() {
        if (readResultsProcessingExecutor != null) {
            readResultsProcessingExecutor.shutdownNow();
        }
    }

    protected <T> ListenableFuture<T> getFuture(JnksIotResultSetFuture future, java.util.function.Function<JnksIotResultSet, T> transformer) {
        return Futures.transform(future, new Function<JnksIotResultSet, T>() {
            @Nullable
            @Override
            public T apply(@Nullable JnksIotResultSet input) {
                return transformer.apply(input);
            }
        }, readResultsProcessingExecutor);
    }

    protected <T> ListenableFuture<T> getFutureAsync(JnksIotResultSetFuture future, com.google.common.util.concurrent.AsyncFunction<JnksIotResultSet, T> transformer) {
        return Futures.transformAsync(future, new AsyncFunction<JnksIotResultSet, T>() {
            @Nullable
            @Override
            public ListenableFuture<T> apply(@Nullable JnksIotResultSet input) {
                try {
                    return transformer.apply(input);
                } catch (Exception e) {
                    return Futures.immediateFailedFuture(e);
                }
            }
        }, readResultsProcessingExecutor);
    }

}
