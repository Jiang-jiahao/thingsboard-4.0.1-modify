package com.jnks.iot.server.dao.sql;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

public interface JnksIotSqlQueue<E, R> {

    void init(ScheduledLogExecutorComponent logExecutor, Function<List<E>, List<R>> saveFunction, Comparator<E> batchUpdateComparator, Function<List<JnksIotSqlQueueElement<E, R>>, List<JnksIotSqlQueueElement<E, R>>> filter, int queueIndex);

    void destroy();

    ListenableFuture<R> add(E element);
}
