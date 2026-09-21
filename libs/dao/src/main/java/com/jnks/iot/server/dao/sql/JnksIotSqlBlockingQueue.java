package com.jnks.iot.server.dao.sql;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JnksIotThreadFactory;
import com.jnks.iot.server.common.data.util.CollectionsUtil;
import com.jnks.iot.server.common.stats.MessagesStats;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class JnksIotSqlBlockingQueue<E, R> implements JnksIotSqlQueue<E, R> {

    private final BlockingQueue<JnksIotSqlQueueElement<E, R>> queue = new LinkedBlockingQueue<>();
    private final JnksIotSqlBlockingQueueParams params;

    private ExecutorService executor;
    private final MessagesStats stats;

    public JnksIotSqlBlockingQueue(JnksIotSqlBlockingQueueParams params, MessagesStats stats) {
        this.params = params;
        this.stats = stats;
    }

    @Override
    public void init(ScheduledLogExecutorComponent logExecutor, Function<List<E>, List<R>> saveFunction, Comparator<E> batchUpdateComparator, Function<List<JnksIotSqlQueueElement<E, R>>, List<JnksIotSqlQueueElement<E, R>>> filter, int index) {
        executor = Executors.newSingleThreadExecutor(JnksIotThreadFactory.forName("sql-queue-" + index + "-" + params.getLogName().toLowerCase()));
        executor.submit(() -> {
            String logName = params.getLogName();
            int batchSize = params.getBatchSize();
            long maxDelay = params.getMaxDelay();
            final List<JnksIotSqlQueueElement<E, R>> entities = new ArrayList<>(batchSize);
            while (!Thread.interrupted()) {
                try {
                    long currentTs = System.currentTimeMillis();
                    JnksIotSqlQueueElement<E, R> attr = queue.poll(maxDelay, TimeUnit.MILLISECONDS);
                    if (attr == null) {
                        continue;
                    } else {
                        entities.add(attr);
                    }
                    queue.drainTo(entities, batchSize - 1);
                    boolean fullPack = entities.size() == batchSize;
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Going to save {} entities", logName, entities.size());
                        log.trace("[{}] Going to save entities: {}", logName, entities);
                    }

                    List<JnksIotSqlQueueElement<E, R>> entitiesToSave = filter.apply(entities);

                    if (params.isBatchSortEnabled()) {
                        entitiesToSave = entitiesToSave.stream().sorted((o1, o2) -> batchUpdateComparator.compare(o1.getEntity(), o2.getEntity())).toList();
                    }

                    List<R> result = saveFunction.apply(entitiesToSave.stream().map(JnksIotSqlQueueElement::getEntity).collect(Collectors.toList()));

                    if (params.isWithResponse()) {
                        for (int i = 0; i < entitiesToSave.size(); i++) {
                            entitiesToSave.get(i).getFuture().set(result.get(i));
                        }

                        if (entities.size() > entitiesToSave.size()) {
                            CollectionsUtil.diffLists(entitiesToSave, entities).forEach(v -> v.getFuture().set(null));
                        }
                    } else {
                        entities.forEach(v -> v.getFuture().set(null));
                    }

                    stats.incrementSuccessful(entities.size());
                    if (!fullPack) {
                        long remainingDelay = maxDelay - (System.currentTimeMillis() - currentTs);
                        if (remainingDelay > 0) {
                            Thread.sleep(remainingDelay);
                        }
                    }
                } catch (Throwable t) {
                    if (t instanceof InterruptedException) {
                        log.info("[{}] Queue polling was interrupted", logName);
                        break;
                    } else {
                        log.error("[{}] Failed to save {} entities", logName, entities.size(), t);
                        try {
                            stats.incrementFailed(entities.size());
                            entities.forEach(entityFutureWrapper -> entityFutureWrapper.getFuture().setException(t));
                        } catch (Throwable th) {
                            log.error("[{}] Failed to set future exception", logName, th);
                        }
                    }
                } finally {
                    entities.clear();
                }
            }
            log.info("[{}] Queue polling completed", logName);
        });

        logExecutor.scheduleAtFixedRate(() -> {
            if (!queue.isEmpty() || stats.getTotal() > 0 || stats.getSuccessful() > 0 || stats.getFailed() > 0) {
                log.info("Queue-{} [{}] queueSize [{}] totalAdded [{}] totalSaved [{}] totalFailed [{}]", index,
                        params.getLogName(), queue.size(), stats.getTotal(), stats.getSuccessful(), stats.getFailed());
                stats.reset();
            }
        }, params.getStatsPrintIntervalMs(), params.getStatsPrintIntervalMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void destroy() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public ListenableFuture<R> add(E element) {
        SettableFuture<R> future = SettableFuture.create();
        queue.add(new JnksIotSqlQueueElement<>(future, element));
        stats.incrementTotal();
        return future;
    }
}
