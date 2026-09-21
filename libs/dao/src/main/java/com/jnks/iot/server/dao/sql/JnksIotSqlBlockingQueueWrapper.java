package com.jnks.iot.server.dao.sql;

import com.google.common.util.concurrent.ListenableFuture;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.stats.MessagesStats;
import com.jnks.iot.server.common.stats.StatsFactory;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
@Data
public class JnksIotSqlBlockingQueueWrapper<E, R> {
    private final CopyOnWriteArrayList<JnksIotSqlBlockingQueue<E, R>> queues = new CopyOnWriteArrayList<>();
    private final JnksIotSqlBlockingQueueParams params;
    private final Function<E, Integer> hashCodeFunction;
    private final int maxThreads;
    private final StatsFactory statsFactory;

    /**
     * Starts JnksIotSqlBlockingQueues.
     *
     * @param  logExecutor  executor that will be printing logs and statistics
     * @param  saveFunction function to save entities in database
     * @param  batchUpdateComparator comparator to sort entities by primary key to avoid deadlocks in cluster mode
     *                               NOTE: you must use all of primary key parts in your comparator
     */
    public void init(ScheduledLogExecutorComponent logExecutor, Consumer<List<E>> saveFunction, Comparator<E> batchUpdateComparator) {
        init(logExecutor, l -> { saveFunction.accept(l); return null; }, batchUpdateComparator, l -> l);
    }

    public void init(ScheduledLogExecutorComponent logExecutor, Function<List<E>, List<R>> saveFunction, Comparator<E> batchUpdateComparator, Function<List<JnksIotSqlQueueElement<E, R>>, List<JnksIotSqlQueueElement<E, R>>> filter) {
        for (int i = 0; i < maxThreads; i++) {
            MessagesStats stats = statsFactory.createMessagesStats(params.getStatsNamePrefix() + ".queue." + i);
            JnksIotSqlBlockingQueue<E, R> queue = new JnksIotSqlBlockingQueue<>(params, stats);
            queues.add(queue);
            queue.init(logExecutor, saveFunction, batchUpdateComparator, filter, i);
        }
    }

    public ListenableFuture<R> add(E element) {
        int queueIndex = element != null ? (hashCodeFunction.apply(element) & 0x7FFFFFFF) % maxThreads : 0;
        return queues.get(queueIndex).add(element);
    }

    public void destroy() {
        queues.forEach(JnksIotSqlBlockingQueue::destroy);
    }
}
