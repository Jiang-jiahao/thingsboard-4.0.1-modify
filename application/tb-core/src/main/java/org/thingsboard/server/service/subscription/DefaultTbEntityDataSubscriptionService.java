/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.subscription;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.common.util.ThingsBoardThreadFactory;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.BaseReadTsKvQuery;
import org.thingsboard.server.common.data.kv.ReadTsKvQuery;
import org.thingsboard.server.common.data.kv.ReadTsKvQueryResult;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.query.AlarmDataQuery;
import org.thingsboard.server.common.data.query.ComparisonTsValue;
import org.thingsboard.server.common.data.query.EntityData;
import org.thingsboard.server.common.data.query.EntityDataQuery;
import org.thingsboard.server.common.data.query.EntityKey;
import org.thingsboard.server.common.data.query.EntityKeyType;
import org.thingsboard.server.common.data.query.TsValue;
import org.thingsboard.server.common.msg.tools.TbRateLimitsException;
import org.thingsboard.server.dao.alarm.AlarmService;
import org.thingsboard.server.dao.attributes.AttributesService;
import org.thingsboard.server.dao.entity.EntityService;
import org.thingsboard.server.dao.timeseries.TimeseriesService;
import org.thingsboard.server.queue.discovery.TbServiceInfoProvider;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.executors.DbCallbackExecutorService;
import org.thingsboard.server.service.ws.WebSocketService;
import org.thingsboard.server.service.ws.WebSocketSessionRef;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.AggHistoryCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.AggKey;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.AggTimeSeriesCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.AlarmCountCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.AlarmCountUpdate;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.AlarmDataCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.AlarmDataUpdate;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.AlarmStatusCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.EntityCountCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.EntityDataCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.EntityDataUpdate;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.EntityHistoryCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.GetTsCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.LatestValueCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.TimeSeriesCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.UnsubscribeCmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Dashboard WebSocket <b>v2 查询型</b>订阅的实现：实体表、实体计数、告警表、告警计数、告警状态。
 *
 * <p>由 {@link org.thingsboard.server.service.ws.DefaultWebSocketService} 按命令类型转过来。
 * 本类不直接写 socket，查完或增量到达后通过 {@link WebSocketService#sendUpdate} 下发；
 * 真正往本地订阅柜子里登记「某个实体的属性/遥测变了要回调」的，是 {@link TbLocalSubscriptionService}。
 *
 * <h2>和 v1 的差别</h2>
 * v1（属性/遥测订阅）一条前端 {@code cmdId} 对应柜子里一条 {@code TbSubscription}。
 * v2 一条 {@code cmdId} 先对应本类的一个 {@link TbAbstractSubCtx}（实体查询上下文），
 * 查询结果里有多少实体、多少类 key，ctx 就会往柜子里塞多少条内部订阅。
 * 因此本类按 {@code cmdId} 索引 ctx；柜子按服务器 {@code sessionSubIdSeq} 索引内部订阅。
 *
 * <h2>本类这张表 vs 订阅柜子</h2>
 * <ul>
 *   <li>{@link #subscriptionsBySessionId}：{@code sessionId → (cmdId → ctx)}，一对一，取消命令时靠它找到 ctx；</li>
 *   <li>{@code DefaultTbLocalSubscriptionService.subscriptionsBySessionId}：
 *       {@code sessionId → (内部 subscriptionId → TbSubscription)}，v1/v2 共用，一条 v2 命令可占多格。</li>
 * </ul>
 *
 * <h2>EntityDataCmd 子命令</h2>
 * 一条实体数据命令可同时带 query 和若干子命令。有 query 时先查出本页实体列表；
 * 有聚合/历史子命令时先查库再推；latest/时序订阅放在它们完成之后，避免快照和增量乱序。
 * 同一 {@code cmdId} 再次到达视为「更新」：清掉旧的实体级内部订阅再按新命令重建。
 *
 * <h2>动态页</h2>
 * query 的 pageLink 带 {@code dynamic=true}（或告警时间窗 &gt; 0）时，
 * {@link #scheduler} 按 {@code server.ws.dynamic_page_link.refresh_interval} 周期性重查，
 * 以便设备进出过滤条件时刷新表格，而不只靠属性增量。
 *
 * @see TbEntityDataSubscriptionService
 * @see TbEntityDataSubCtx
 * @see TbLocalSubscriptionService
 */
@SuppressWarnings("UnstableApiUsage")
@Slf4j
@TbCoreComponent
@Service
public class DefaultTbEntityDataSubscriptionService implements TbEntityDataSubscriptionService {

    /** 历史/窗口查询未指定 limit 时的默认条数。 */
    private static final int DEFAULT_LIMIT = 100;

    /**
     * v2 命令级上下文：sessionId → (前端 cmdId → ctx)。
     * 与本地订阅柜子不是同一张表；这里一对一，柜子里才是一对多。
     */
    private final ConcurrentMap<String, ConcurrentMap<Integer, TbAbstractSubCtx>> subscriptionsBySessionId = new ConcurrentHashMap<>();

    /** 下行门面。用 {@link Lazy} 打破与 {@link org.thingsboard.server.service.ws.DefaultWebSocketService} 的循环依赖。 */
    @Autowired
    @Lazy
    private WebSocketService wsService;

    /** 按 EntityDataQuery / 计数 query 查实体列表。 */
    @Autowired
    private EntityService entityService;

    /** 告警列表、告警计数、活跃告警查询。 */
    @Autowired
    private AlarmService alarmService;

    /** 属性读取，供 ctx 补最新值或建属性订阅。 */
    @Autowired
    private AttributesService attributesService;

    /**
     * 本地订阅柜子。ctx 为每个实体/key 类型 {@code addSubscription} 进去；
     * 属性/遥测变更后再回调 ctx，由 ctx 聚合成一条带 cmdId 的 {@code EntityDataUpdate}。
     */
    @Autowired
    @Lazy
    private TbLocalSubscriptionService localSubscriptionService;

    /** 时序查询：历史窗口、聚合、混合存储下补 latest。 */
    @Autowired
    private TimeseriesService tsService;

    /** 本 Core 节点 id，写入内部 {@code TbSubscription}，集群内识别归属。 */
    @Autowired
    private TbServiceInfoProvider serviceInfoProvider;

    /** 查库回调线程池，部分 DAO 异步结果在此继续。 */
    @Autowired
    @Getter
    private DbCallbackExecutorService dbCallbackExecutor;

    /**
     * 动态页 / 告警窗刷新定时器。
     * {@code refresh_pool_size=1} 时单线程，否则固定大小池，避免大量 dynamic query 互相堵住。
     */
    private ScheduledExecutorService scheduler;

    /**
     * 时序存储类型。{@code sql}/{@code timescale} 时 latest 已在 SQL 侧，
     * {@link #handleLatestCmd} 不必再打 Cassandra/NoSQL 补洞。
     */
    @Value("${database.ts.type}")
    private String databaseTsType;

    /** 动态页刷新间隔（秒），默认 6。 */
    @Value("${server.ws.dynamic_page_link.refresh_interval:6}")
    private long dynamicPageLinkRefreshInterval;

    /** 动态页调度线程数。1 走单线程 executor。 */
    @Value("${server.ws.dynamic_page_link.refresh_pool_size:1}")
    private int dynamicPageLinkRefreshPoolSize;

    /** 单条实体数据订阅最多追踪的实体数，超出由 ctx 截断。 */
    @Value("${server.ws.max_entities_per_data_subscription:1000}")
    private int maxEntitiesPerDataSubscription;

    /** 单条告警数据/计数订阅最多追踪的实体数。 */
    @Value("${server.ws.max_entities_per_alarm_subscription:1000}")
    private int maxEntitiesPerAlarmSubscription;

    /** 每个刷新周期内允许的告警查询次数上限，防止告警表把库打满。 */
    @Value("${server.ws.dynamic_page_link.max_alarm_queries_per_refresh_interval:10}")
    private int maxAlarmQueriesPerRefreshInterval;

    /** UI 侧单次历史点上限，写入配置供查询 limit 参考（本类 {@link #getLimit} 仍以命令 limit 为主）。 */
    @Value("${ui.dashboard.max_datapoints_limit:50000}")
    private int maxDatapointLimit;

    /** 告警状态订阅缓存的活跃告警条数上限。 */
    @Value("${server.ws.alarms_per_alarm_status_subscription_cache_size:10}")
    private int alarmsPerAlarmStatusSubscriptionCacheSize;

    /**
     * 时序/聚合查询完成后的回调线程。单线程，保证同一 ctx 上
     * 「填 EntityData → 加锁推 WS → 建内部订阅」串行，避免快照与增量交错。
     */
    private ExecutorService wsCallBackExecutor;

    /** {@code true} 表示 latest 遥测已在 SQL，handleLatestCmd 可跳过补查。 */
    private boolean tsInSqlDB;

    /** 当前节点 serviceId，传给各 ctx。 */
    private String serviceId;

    /** 常规查询 / 动态查询 / 告警查询的次数与耗时，供 {@link #printStats} 打印。 */
    private SubscriptionServiceStatistics stats = new SubscriptionServiceStatistics();

    /**
     * 启动：记录 serviceId、创建 WS 回调单线程池、判断时序是否在 SQL、按配置建动态页调度器。
     */
    @PostConstruct
    public void initExecutor() {
        serviceId = serviceInfoProvider.getServiceId();
        wsCallBackExecutor = Executors.newSingleThreadExecutor(ThingsBoardThreadFactory.forName("ws-entity-sub-callback"));
        tsInSqlDB = databaseTsType.equalsIgnoreCase("sql") || databaseTsType.equalsIgnoreCase("timescale");
        if (dynamicPageLinkRefreshPoolSize == 1) {
            scheduler = ThingsBoardExecutors.newSingleThreadScheduledExecutor("ws-entity-sub-scheduler");
        } else {
            scheduler = ThingsBoardExecutors.newScheduledThreadPool(dynamicPageLinkRefreshPoolSize, "ws-entity-sub-scheduler");
        }
    }

    /**
     * 停机立即打断回调线程与调度器。进行中的查库回调可能被丢弃，会话清理走 {@link #cancelAllSessionSubscriptions}。
     */
    @PreDestroy
    public void shutdownExecutor() {
        if (wsCallBackExecutor != null) {
            wsCallBackExecutor.shutdownNow();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    /**
     * 处理实体数据命令（Dashboard 实体表 / 时序图）。
     *
     * <ol>
     *   <li>按 sessionId+cmdId 取或建 {@link TbEntityDataSubCtx}。已存在且带了子命令则先
     *       {@code clearEntitySubscriptions}，避免旧内部订阅继续推。</li>
     *   <li>有 query：解析（含动态值）、把 latestCmd 的 key 并进 query.latestValues、{@code fetchData} 拉本页实体。
     *       动态 pageLink 则挂固定间隔刷新任务。</li>
     *   <li>聚合历史 / 聚合时序 / 普通历史若存在，先异步查完再 {@link #handleRegularCommands}（latest + 实时时序）；
     *       都没有则直接走常规命令。这样前端先看到历史/聚合快照，再收到增量。</li>
     * </ol>
     *
     * <p>限流异常只记日志；其它运行时异常会 {@link WebSocketService#close} 整条会话（{@code SERVICE_RESTARTED}）。
     */
    @Override
    public void handleCmd(WebSocketSessionRef session, EntityDataCmd cmd) {
        TbEntityDataSubCtx ctx = getSubCtx(session.getSessionId(), cmd.getCmdId());
        if (ctx != null) {
            log.debug("[{}][{}] Updating existing subscriptions using: {}", session.getSessionId(), cmd.getCmdId(), cmd);
            if (cmd.hasAnyCmd()) {
                // 除了单纯query命令，其他都需要清除订阅
                ctx.clearEntitySubscriptions();
            }
        } else {
            log.debug("[{}][{}] Creating new subscription using: {}", session.getSessionId(), cmd.getCmdId(), cmd);
            ctx = createSubCtx(session, cmd);
        }
        ctx.setCurrentCmd(cmd);

        // 有 query 才重新拉实体列表；仅带子命令、query 为 null 时复用 ctx 里已有的本页数据。
        if (cmd.getQuery() != null) {
            if (ctx.getQuery() == null) {
                log.debug("[{}][{}] Initializing data using query: {}", session.getSessionId(), cmd.getCmdId(), cmd.getQuery());
            } else {
                log.debug("[{}][{}] Updating data using query: {}", session.getSessionId(), cmd.getCmdId(), cmd.getQuery());
            }
            ctx.setAndResolveQuery(cmd.getQuery());
            EntityDataQuery query = ctx.getQuery();
            // latest 订阅的 key 也要出现在 query.latestValues 里，否则 fetchData 结果不含这些列，后续订阅对不上。
            if (cmd.getLatestCmd() != null) {
                cmd.getLatestCmd().getKeys().forEach(key -> {
                    if (!query.getLatestValues().contains(key)) {
                        query.getLatestValues().add(key);
                    }
                });
            }
            long start = System.currentTimeMillis();
            ctx.fetchData();
            long end = System.currentTimeMillis();
            stats.getRegularQueryInvocationCnt().incrementAndGet();
            stats.getRegularQueryTimeSpent().addAndGet(end - start);
            ctx.cancelTasks();
            if (ctx.getQuery().getPageLink().isDynamic()) {
                //TODO: validate number of dynamic page links against rate limits. Ignore dynamic flag if limit is reached.
                TbEntityDataSubCtx finalCtx = ctx;
                ScheduledFuture<?> task = scheduler.scheduleWithFixedDelay(
                        () -> refreshDynamicQuery(finalCtx),
                        dynamicPageLinkRefreshInterval, dynamicPageLinkRefreshInterval, TimeUnit.SECONDS);
                finalCtx.setRefreshTask(task);
            }
        }

        try {
            List<ListenableFuture<?>> cmdFutures = new ArrayList<>();
            if (cmd.getAggHistoryCmd() != null) {
                cmdFutures.add(handleAggHistoryCmd(ctx, cmd.getAggHistoryCmd()));
            }
            if (cmd.getAggTsCmd() != null) {
                cmdFutures.add(handleAggTsCmd(ctx, cmd.getAggTsCmd()));
            }
            if (cmd.getHistoryCmd() != null) {
                cmdFutures.add(handleHistoryCmd(ctx, cmd.getHistoryCmd()));
            }
            if (cmdFutures.isEmpty()) {
                handleRegularCommands(ctx, cmd);
            } else {
                TbEntityDataSubCtx finalCtx = ctx;
                Futures.addCallback(Futures.allAsList(cmdFutures), new FutureCallback<>() {
                    @Override
                    public void onSuccess(@Nullable List<Object> result) {
                        // 对于需要查询历史数据的等待历史数据查询完成后回调
                        handleRegularCommands(finalCtx, cmd);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        log.warn("[{}][{}] Failed to process command", finalCtx.getSessionId(), finalCtx.getCmdId());
                    }
                }, wsCallBackExecutor);
            }
        } catch (RuntimeException e) {
            handleWsCmdRuntimeException(ctx.getSessionId(), e, cmd);
        }
    }

    /**
     * 历史/聚合查完（或没有这些子命令）后处理「长期」部分：latest 快照+订阅、时序窗口+订阅。
     * 两者都没有则只把 fetchData 的本页实体作为初始包发出（纯实体列表、不订遥测）。
     */
    private void handleRegularCommands(TbEntityDataSubCtx ctx, EntityDataCmd cmd) {
        try {
            if (cmd.getLatestCmd() != null || cmd.getTsCmd() != null) {
                if (cmd.getLatestCmd() != null) {
                    handleLatestCmd(ctx, cmd.getLatestCmd());
                }
                if (cmd.getTsCmd() != null) {
                    handleTimeSeriesCmd(ctx, cmd.getTsCmd());
                }
            } else {
                checkAndSendInitialData(ctx);
            }
        } catch (RuntimeException e) {
            handleWsCmdRuntimeException(ctx.getSessionId(), e, cmd);
        }
    }

    /**
     * 尚未下发过初始包时，把 ctx 里整页 {@code PageData<EntityData>} 推出去并打标。
     * 历史/聚合路径会自己组 {@link EntityDataUpdate}，避免这里再推一遍空时序。
     */
    private void checkAndSendInitialData(@Nullable TbEntityDataSubCtx theCtx) {
        if (!theCtx.isInitialDataSent()) {
            EntityDataUpdate update = new EntityDataUpdate(theCtx.getCmdId(), theCtx.getData(), null, theCtx.getMaxEntitiesPerDataSubscription());
            theCtx.sendWsMsg(update);
            theCtx.setInitialDataSent(true);
        }
    }

    /**
     * 聚合历史：每个 {@link AggKey} 查当前窗口一块聚合值；若带了 previous 时间范围再查一块对比值。
     * {@code previousValueOnly=true} 时跳过当前窗口。不建立实时订阅（subscribe=false）。
     */
    private ListenableFuture<TbEntityDataSubCtx> handleAggHistoryCmd(TbEntityDataSubCtx ctx, AggHistoryCmd cmd) {
        ConcurrentMap<Integer, ReadTsKvQueryInfo> queries = new ConcurrentHashMap<>();
        for (AggKey key : cmd.getKeys()) {
            if (key.getPreviousValueOnly() == null || !key.getPreviousValueOnly()) {
                var query = new BaseReadTsKvQuery(key.getKey(), cmd.getStartTs(), cmd.getEndTs(), cmd.getEndTs() - cmd.getStartTs(), 1, key.getAgg());
                queries.put(query.getId(), new ReadTsKvQueryInfo(key, query, false));
            }
            if (key.getPreviousStartTs() != null && key.getPreviousEndTs() != null && key.getPreviousEndTs() >= key.getPreviousStartTs()) {
                var query = new BaseReadTsKvQuery(key.getKey(), key.getPreviousStartTs(), key.getPreviousEndTs(), key.getPreviousEndTs() - key.getPreviousStartTs(), 1, key.getAgg());
                queries.put(query.getId(), new ReadTsKvQueryInfo(key, query, true));
            }
        }
        return handleAggCmd(ctx, cmd.getKeys(), queries, cmd.getStartTs(), cmd.getEndTs(), false);
    }

    /**
     * 聚合时序窗口：每个 key 查 [startTs, startTs+timeWindow] 一块聚合，查完后按 lastTs 建立时序订阅（subscribe=true）。
     */
    private ListenableFuture<TbEntityDataSubCtx> handleAggTsCmd(TbEntityDataSubCtx ctx, AggTimeSeriesCmd cmd) {
        ConcurrentMap<Integer, ReadTsKvQueryInfo> queries = new ConcurrentHashMap<>();
        for (AggKey key : cmd.getKeys()) {
            var query = new BaseReadTsKvQuery(key.getKey(), cmd.getStartTs(), cmd.getStartTs() + cmd.getTimeWindow(), cmd.getTimeWindow(), 1, key.getAgg());
            queries.put(query.getId(), new ReadTsKvQueryInfo(key, query, false));
        }
        return handleAggCmd(ctx, cmd.getKeys(), queries, cmd.getStartTs(), cmd.getStartTs() + cmd.getTimeWindow(), true);
    }

    /**
     * 对本页每个实体并行跑同一组 {@link ReadTsKvQuery}，把结果填进 {@code entityData.aggLatest}（当前值 / 对比值）。
     *
     * <p>全部完成后在 ctx 的 {@code wsLock} 里：第一次推整页，之后只推本页实体增量；
     * {@code subscribe=true} 时用各 key 最后时间戳建时序内部订阅。推完 {@code clearTsAndAggData}，避免下次把旧点再带出去。
     */
    private ListenableFuture<TbEntityDataSubCtx> handleAggCmd(TbEntityDataSubCtx ctx, List<AggKey> keys, ConcurrentMap<Integer, ReadTsKvQueryInfo> queries,
                                                              long startTs, long endTs, boolean subscribe) {
        Map<EntityData, ListenableFuture<List<ReadTsKvQueryResult>>> fetchResultMap = new HashMap<>();
        List<EntityData> entityDataList = ctx.getData().getData();
        List<ReadTsKvQuery> queryList = queries.values().stream().map(ReadTsKvQueryInfo::getQuery).collect(Collectors.toList());
        entityDataList.forEach(entityData -> fetchResultMap.put(entityData,
                tsService.findAllByQueries(ctx.getTenantId(), entityData.getEntityId(), queryList)));
        return Futures.transform(Futures.allAsList(fetchResultMap.values()), f -> {
            // 每个实体每个 key 的最后点时间，给后续时序订阅做增量起点。
            Map<EntityData, Map<String, Long>> lastTsEntityMap = new HashMap<>();
            fetchResultMap.forEach((entityData, future) -> {
                try {
                    Map<String, Long> lastTsMap = new HashMap<>();
                    lastTsEntityMap.put(entityData, lastTsMap);

                    List<ReadTsKvQueryResult> queryResults = future.get();
                    if (queryResults != null) {
                        for (ReadTsKvQueryResult queryResult : queryResults) {
                            ReadTsKvQueryInfo queryInfo = queries.get(queryResult.getQueryId());
                            ComparisonTsValue comparisonTsValue = entityData.getAggLatest().computeIfAbsent(queryInfo.getKey().getId(), agg -> new ComparisonTsValue());
                            if (queryInfo.isPrevious()) {
                                comparisonTsValue.setPrevious(queryResult.toTsValue(queryInfo.getQuery()));
                            } else {
                                comparisonTsValue.setCurrent(queryResult.toTsValue(queryInfo.getQuery()));
                                lastTsMap.put(queryInfo.getQuery().getKey(), queryResult.getLastEntryTs());
                            }
                        }
                    }
                    // 库里没有的 key 也占一个空 ComparisonTsValue，前端列对齐。
                    keys.forEach(key -> {
                        entityData.getAggLatest().putIfAbsent(key.getId(), new ComparisonTsValue(TsValue.EMPTY, TsValue.EMPTY));
                    });
                } catch (InterruptedException | ExecutionException e) {
                    log.warn("[{}][{}][{}] Failed to fetch historical data", ctx.getSessionId(), ctx.getCmdId(), entityData.getEntityId(), e);
                    ctx.sendWsMsg(new EntityDataUpdate(ctx.getCmdId(), SubscriptionErrorCode.INTERNAL_ERROR.getCode(), "Failed to fetch historical data!"));
                }
            });
            ctx.getWsLock().lock();
            try {
                EntityDataUpdate update;
                if (!ctx.isInitialDataSent()) {
                    update = new EntityDataUpdate(ctx.getCmdId(), ctx.getData(), null, ctx.getMaxEntitiesPerDataSubscription());
                    ctx.setInitialDataSent(true);
                } else {
                    update = new EntityDataUpdate(ctx.getCmdId(), null, entityDataList, ctx.getMaxEntitiesPerDataSubscription());
                }
                if (subscribe) {
                    ctx.createTimeSeriesSubscriptions(lastTsEntityMap, startTs, endTs, true);
                }
                ctx.sendWsMsg(update);
                entityDataList.forEach(EntityData::clearTsAndAggData);
            } finally {
                ctx.getWsLock().unlock();
            }
            return ctx;
        }, wsCallBackExecutor);
    }

    /**
     * 实体数据命令处理中的未捕获运行时异常。限流不关连接；其它情况关掉整条 WS，
     * 前端会重连（状态码借用 SERVICE_RESTARTED）。
     */
    private void handleWsCmdRuntimeException(String sessionId, RuntimeException e, EntityDataCmd cmd) {
        log.debug("[{}] Failed to process ws cmd: {}", sessionId, cmd, e);
        if (e instanceof TbRateLimitsException) {
            return;
        }
        wsService.close(sessionId, CloseStatus.SERVICE_RESTARTED);
    }

    /**
     * 实体计数：按 query 查当前数量并推给前端，然后挂动态刷新。
     * 同一 cmdId 再来视为重复，忽略（不像 EntityDataCmd 那样支持更新）。
     */
    @Override
    public void handleCmd(WebSocketSessionRef session, EntityCountCmd cmd) {
        TbEntityCountSubCtx ctx = getSubCtx(session.getSessionId(), cmd.getCmdId());
        if (ctx == null) {
            ctx = createSubCtx(session, cmd);
            long start = System.currentTimeMillis();
            ctx.fetchData();
            long end = System.currentTimeMillis();
            stats.getRegularQueryInvocationCnt().incrementAndGet();
            stats.getRegularQueryTimeSpent().addAndGet(end - start);
            TbEntityCountSubCtx finalCtx = ctx;
            ScheduledFuture<?> task = scheduler.scheduleWithFixedDelay(
                    () -> refreshDynamicQuery(finalCtx),
                    dynamicPageLinkRefreshInterval, dynamicPageLinkRefreshInterval, TimeUnit.SECONDS);
            finalCtx.setRefreshTask(task);
        } else {
            log.debug("[{}][{}] Received duplicate command: {}", session.getSessionId(), cmd.getCmdId(), cmd);
        }
    }

    /**
     * 告警数据表：查出关联实体 → 拉告警页 → 给实体建 latest 订阅（告警行上的设备属性列）。
     * 时间窗 &gt; 0 时定时 {@link #refreshAlarmQuery}（内部有每周期查询次数上限）。
     * 同一 cmdId 再来会覆盖 query、清空旧内部订阅后重拉，相当于刷新。
     */
    @Override
    public void handleCmd(WebSocketSessionRef session, AlarmDataCmd cmd) {
        TbAlarmDataSubCtx ctx = getSubCtx(session.getSessionId(), cmd.getCmdId());
        if (ctx == null) {
            log.debug("[{}][{}] Creating new alarm subscription using: {}", session.getSessionId(), cmd.getCmdId(), cmd);
            ctx = createSubCtx(session, cmd);
        }
        ctx.setAndResolveQuery(cmd.getQuery());
        AlarmDataQuery adq = ctx.getQuery();
        long start = System.currentTimeMillis();
        ctx.fetchData();
        long end = System.currentTimeMillis();
        stats.getRegularQueryInvocationCnt().incrementAndGet();
        stats.getRegularQueryTimeSpent().addAndGet(end - start);
        List<EntityData> entities = ctx.getEntitiesData();
        ctx.cancelTasks();
        ctx.clearEntitySubscriptions();
        if (entities.isEmpty()) {
            AlarmDataUpdate update = new AlarmDataUpdate(cmd.getCmdId(), new PageData<>(), null, 0, 0);
            ctx.sendWsMsg(update);
        } else {
            ctx.fetchAlarms();
            ctx.createLatestValuesSubscriptions(cmd.getQuery().getLatestValues());
            if (adq.getPageLink().getTimeWindow() > 0) {
                TbAlarmDataSubCtx finalCtx = ctx;
                ScheduledFuture<?> task = scheduler.scheduleWithFixedDelay(
                        () -> refreshAlarmQuery(finalCtx), dynamicPageLinkRefreshInterval, dynamicPageLinkRefreshInterval, TimeUnit.SECONDS);
                finalCtx.setRefreshTask(task);
            }
        }
    }

    /**
     * 告警计数：先按 query 得到实体集合（可为 null，表示不按实体过滤），再查数量。
     * 有实体集合则给每个实体建告警订阅，之后动态刷新重查。空集合直接推 0。重复 cmdId 忽略。
     */
    @Override
    public void handleCmd(WebSocketSessionRef session, AlarmCountCmd cmd) {
        TbAlarmCountSubCtx ctx = getSubCtx(session.getSessionId(), cmd.getCmdId());
        if (ctx == null) {
            ctx = createSubCtx(session, cmd);
            long start = System.currentTimeMillis();
            ctx.fetchData();
            long end = System.currentTimeMillis();
            stats.getRegularQueryInvocationCnt().incrementAndGet();
            stats.getRegularQueryTimeSpent().addAndGet(end - start);
            Set<EntityId> entitiesIds = ctx.getEntitiesIds();
            ctx.cancelTasks();
            ctx.clearAlarmSubscriptions();
            if (entitiesIds != null && entitiesIds.isEmpty()) {
                AlarmCountUpdate update = new AlarmCountUpdate(cmd.getCmdId(), 0);
                ctx.sendWsMsg(update);
            } else {
                ctx.doFetchAlarmCount();
                if (entitiesIds != null) {
                    ctx.createAlarmSubscriptions();
                }
                TbAlarmCountSubCtx finalCtx = ctx;
                ScheduledFuture<?> task = scheduler.scheduleWithFixedDelay(
                        () -> refreshDynamicQuery(finalCtx),
                        dynamicPageLinkRefreshInterval, dynamicPageLinkRefreshInterval, TimeUnit.SECONDS);
                finalCtx.setRefreshTask(task);
            }
        } else {
            log.debug("[{}][{}] Received duplicate command: {}", session.getSessionId(), cmd.getCmdId(), cmd);
        }
    }

    /**
     * 某实体当前是否有活跃告警（状态灯）。建 ctx 时就会 {@code createSubscription} 进柜子，
     * 再查一遍活跃告警缓存后 {@code sendUpdate}。重复 cmdId 忽略。
     */
    @Override
    public void handleCmd(WebSocketSessionRef session, AlarmStatusCmd cmd) {
        log.debug("[{}] Handling alarm status subscription cmd (cmdId: {})", session.getSessionId(), cmd.getCmdId());
        TbAlarmStatusSubCtx ctx = getSubCtx(session.getSessionId(), cmd.getCmdId());
        if (ctx == null) {
            ctx = createSubCtx(session, cmd);
            long start = System.currentTimeMillis();
            ctx.fetchActiveAlarms();
            long end = System.currentTimeMillis();
            stats.getAlarmQueryInvocationCnt().incrementAndGet();
            stats.getAlarmQueryTimeSpent().addAndGet(end - start);
            ctx.sendUpdate();
        } else {
            log.debug("[{}][{}] Received duplicate command: {}", session.getSessionId(), cmd.getCmdId(), cmd);
        }
    }

    /**
     * 动态刷新任务执行前的存活检查：ctx 已 stop、会话已从本表移除、cmdId 已取消，都不再刷新并返回 false。
     */
    private boolean validate(TbAbstractSubCtx finalCtx) {
        if (finalCtx.isStopped()) {
            log.warn("[{}][{}][{}] Received validation task for already stopped context.", finalCtx.getTenantId(), finalCtx.getSessionId(), finalCtx.getCmdId());
            return false;
        }
        var cmdMap = subscriptionsBySessionId.get(finalCtx.getSessionId());
        if (cmdMap == null) {
            log.warn("[{}][{}][{}] Received validation task for already removed session.", finalCtx.getTenantId(), finalCtx.getSessionId(), finalCtx.getCmdId());
            return false;
        } else if (!cmdMap.containsKey(finalCtx.getCmdId())) {
            log.warn("[{}][{}][{}] Received validation task for unregistered cmdId.", finalCtx.getTenantId(), finalCtx.getSessionId(), finalCtx.getCmdId());
            return false;
        }
        return true;
    }

    /**
     * 动态页刷新：校验通过则 {@code ctx.update()}（重查实体/计数并视情况重建内部订阅），否则 {@code stop} 摘掉定时任务。
     */
    private void refreshDynamicQuery(TbAbstractEntityQuerySubCtx<?> finalCtx) {
        try {
            if (validate(finalCtx)) {
                long start = System.currentTimeMillis();
                finalCtx.update();
                long end = System.currentTimeMillis();
                log.trace("[{}][{}] Executing query: {}", finalCtx.getSessionId(), finalCtx.getCmdId(), finalCtx.getQuery());
                stats.getDynamicQueryInvocationCnt().incrementAndGet();
                stats.getDynamicQueryTimeSpent().addAndGet(end - start);
            } else {
                finalCtx.stop();
            }
        } catch (Exception e) {
            log.warn("[{}][{}] Failed to refresh query", finalCtx.getSessionId(), finalCtx.getCmdId(), e);
        }
    }

    /**
     * 告警表定时刷新入口。真正是否查库由 ctx 的 invocation 计数与 {@code maxAlarmQueriesPerRefreshInterval} 决定。
     */
    private void refreshAlarmQuery(TbAlarmDataSubCtx finalCtx) {
        if (validate(finalCtx)) {
            finalCtx.checkAndResetInvocationCounter();
        } else {
            finalCtx.stop();
        }
    }

    /**
     * 按 {@code server.ws.dynamic_page_link.stats} 间隔打印查询次数/耗时，以及当前仍挂着的动态 ctx 数量。全 0 不打。
     */
    @Scheduled(fixedDelayString = "${server.ws.dynamic_page_link.stats:10000}")
    public void printStats() {
        int alarmQueryInvocationCntValue = stats.getAlarmQueryInvocationCnt().getAndSet(0);
        long alarmQueryInvocationTimeValue = stats.getAlarmQueryTimeSpent().getAndSet(0);
        int regularQueryInvocationCntValue = stats.getRegularQueryInvocationCnt().getAndSet(0);
        long regularQueryInvocationTimeValue = stats.getRegularQueryTimeSpent().getAndSet(0);
        int dynamicQueryInvocationCntValue = stats.getDynamicQueryInvocationCnt().getAndSet(0);
        long dynamicQueryInvocationTimeValue = stats.getDynamicQueryTimeSpent().getAndSet(0);
        long dynamicQueryCnt = subscriptionsBySessionId.values().stream().mapToLong(m -> m.values().stream().filter(TbAbstractSubCtx::isDynamic).count()).sum();
        if (regularQueryInvocationCntValue > 0 || dynamicQueryInvocationCntValue > 0 || dynamicQueryCnt > 0 || alarmQueryInvocationCntValue > 0) {
            log.info("Stats: regularQueryInvocationCnt = [{}], regularQueryInvocationTime = [{}], " +
                            "dynamicQueryCnt = [{}] dynamicQueryInvocationCnt = [{}], dynamicQueryInvocationTime = [{}], " +
                            "alarmQueryInvocationCnt = [{}], alarmQueryInvocationTime = [{}]",
                    regularQueryInvocationCntValue, regularQueryInvocationTimeValue,
                    dynamicQueryCnt, dynamicQueryInvocationCntValue, dynamicQueryInvocationTimeValue,
                    alarmQueryInvocationCntValue, alarmQueryInvocationTimeValue);
        }
    }

    /**
     * 新建实体数据 ctx，登记到 {@link #subscriptionsBySessionId}。
     * 命令里带了 query 会先 resolve（含动态值占位），真正 fetch 仍在 {@link #handleCmd} 里。
     *
     * <p>TODO: 此处 resolve 与 {@link #handleCmd} 里第二次 {@code setAndResolveQuery} 重复，
     * 见 {@link TbAbstractEntityQuerySubCtx#setAndResolveQuery} 上的退订 TODO。
     */
    private TbEntityDataSubCtx createSubCtx(WebSocketSessionRef sessionRef, EntityDataCmd cmd) {
        Map<Integer, TbAbstractSubCtx> sessionSubs = subscriptionsBySessionId.computeIfAbsent(sessionRef.getSessionId(), k -> new ConcurrentHashMap<>());
        TbEntityDataSubCtx ctx = new TbEntityDataSubCtx(serviceId, wsService, entityService, localSubscriptionService,
                attributesService, stats, sessionRef, cmd.getCmdId(), maxEntitiesPerDataSubscription);
        if (cmd.getQuery() != null) {
            ctx.setAndResolveQuery(cmd.getQuery());
        }
        sessionSubs.put(cmd.getCmdId(), ctx);
        return ctx;
    }

    /** 新建实体计数 ctx 并按 cmdId 登记。 */
    private TbEntityCountSubCtx createSubCtx(WebSocketSessionRef sessionRef, EntityCountCmd cmd) {
        Map<Integer, TbAbstractSubCtx> sessionSubs = subscriptionsBySessionId.computeIfAbsent(sessionRef.getSessionId(), k -> new ConcurrentHashMap<>());
        TbEntityCountSubCtx ctx = new TbEntityCountSubCtx(serviceId, wsService, entityService, localSubscriptionService,
                attributesService, stats, sessionRef, cmd.getCmdId());
        if (cmd.getQuery() != null) {
            ctx.setAndResolveQuery(cmd.getQuery());
        }
        sessionSubs.put(cmd.getCmdId(), ctx);
        return ctx;
    }


    /** 新建告警数据 ctx；query 必有，创建时就 resolve。 */
    private TbAlarmDataSubCtx createSubCtx(WebSocketSessionRef sessionRef, AlarmDataCmd cmd) {
        Map<Integer, TbAbstractSubCtx> sessionSubs = subscriptionsBySessionId.computeIfAbsent(sessionRef.getSessionId(), k -> new ConcurrentHashMap<>());
        TbAlarmDataSubCtx ctx = new TbAlarmDataSubCtx(serviceId, wsService, entityService, localSubscriptionService,
                attributesService, stats, alarmService, sessionRef, cmd.getCmdId(), maxEntitiesPerAlarmSubscription,
                maxAlarmQueriesPerRefreshInterval);
        ctx.setAndResolveQuery(cmd.getQuery());
        sessionSubs.put(cmd.getCmdId(), ctx);
        return ctx;
    }

    /** 新建告警计数 ctx 并登记。 */
    private TbAlarmCountSubCtx createSubCtx(WebSocketSessionRef sessionRef, AlarmCountCmd cmd) {
        Map<Integer, TbAbstractSubCtx> sessionSubs = subscriptionsBySessionId.computeIfAbsent(sessionRef.getSessionId(), k -> new ConcurrentHashMap<>());
        TbAlarmCountSubCtx ctx = new TbAlarmCountSubCtx(serviceId, wsService, entityService, localSubscriptionService,
                attributesService, stats, alarmService, sessionRef, cmd.getCmdId(), maxEntitiesPerAlarmSubscription, maxAlarmQueriesPerRefreshInterval);
        if (cmd.getQuery() != null) {
            ctx.setAndResolveQuery(cmd.getQuery());
        }
        sessionSubs.put(cmd.getCmdId(), ctx);
        return ctx;
    }

    /**
     * 新建告警状态 ctx：构造里就会按命令实体建一条内部告警订阅，再放入 cmdId 索引。
     */
    private TbAlarmStatusSubCtx createSubCtx(WebSocketSessionRef sessionRef, AlarmStatusCmd cmd) {
        Map<Integer, TbAbstractSubCtx> sessionSubs = subscriptionsBySessionId.computeIfAbsent(sessionRef.getSessionId(), k -> new ConcurrentHashMap<>());
        TbAlarmStatusSubCtx ctx = new TbAlarmStatusSubCtx(serviceId, wsService, localSubscriptionService,
                stats, alarmService, alarmsPerAlarmStatusSubscriptionCacheSize, sessionRef, cmd.getCmdId());
        ctx.createSubscription(cmd);
        sessionSubs.put(cmd.getCmdId(), ctx);
        return ctx;
    }

    /**
     * 按外部 sessionId + 前端 cmdId 取 ctx。会话从未建过 v2 命令时返回 null。
     */
    @SuppressWarnings("unchecked")
    private <T extends TbAbstractSubCtx> T getSubCtx(String sessionId, int cmdId) {
        Map<Integer, TbAbstractSubCtx> sessionSubs = subscriptionsBySessionId.get(sessionId);
        if (sessionSubs != null) {
            return (T) sessionSubs.get(cmdId);
        } else {
            return null;
        }
    }

    /** 实时时序窗口：查 [startTs, endTs] 后订阅后续点（{@code subscribe=true}）。 */
    private ListenableFuture<TbEntityDataSubCtx> handleTimeSeriesCmd(TbEntityDataSubCtx ctx, TimeSeriesCmd cmd) {
        log.debug("[{}][{}] Fetching time-series data for last {} ms for keys: ({})", ctx.getSessionId(), ctx.getCmdId(), cmd.getTimeWindow(), cmd.getKeys());
        return handleGetTsCmd(ctx, cmd, true);
    }


    /** 一次性历史：只查窗口，不建时序内部订阅。 */
    private ListenableFuture<TbEntityDataSubCtx> handleHistoryCmd(TbEntityDataSubCtx ctx, EntityHistoryCmd cmd) {
        log.debug("[{}][{}] Fetching history data for start {} and end {} ms for keys: ({})", ctx.getSessionId(), ctx.getCmdId(), cmd.getStartTs(), cmd.getEndTs(), cmd.getKeys());
        return handleGetTsCmd(ctx, cmd, false);
    }

    /**
     * 对本页每个实体按 keys 查时序窗口，填进 {@code entityData.timeseries}。
     *
     * <p>{@code fetchLatestPreviousPoint} 时额外查窗口前一年到 startTs 的各 1 个点，
     * 方便图表在窗口左缘补「前一个点」；结果按 ts 降序排。
     * {@code subscribe=true} 时用各 key 最大 ts 建内部时序订阅。
     * 推送策略与 {@link #handleAggCmd} 相同：首次整页，其后只推实体列表；最后清掉 ts/agg 缓存。
     */
    private ListenableFuture<TbEntityDataSubCtx> handleGetTsCmd(TbEntityDataSubCtx ctx, GetTsCmd cmd, boolean subscribe) {
        Map<Integer, String> queriesKeys = new ConcurrentHashMap<>();

        List<String> keys = cmd.getKeys();
        List<ReadTsKvQuery> finalTsKvQueryList;
        List<ReadTsKvQuery> tsKvQueryList = keys.stream().map(key -> {
            var query = new BaseReadTsKvQuery(key, cmd.getStartTs(), cmd.getEndTs(), cmd.toAggregationParams(), getLimit(cmd.getLimit()));
            queriesKeys.put(query.getId(), query.getKey());
            return query;
        }).collect(Collectors.toList());
        if (cmd.isFetchLatestPreviousPoint()) {
            finalTsKvQueryList = new ArrayList<>(tsKvQueryList);
            finalTsKvQueryList.addAll(keys.stream().map(key -> {
                        var query = new BaseReadTsKvQuery(key, cmd.getStartTs() - TimeUnit.DAYS.toMillis(365), cmd.getStartTs(), cmd.toAggregationParams(), 1);
                        queriesKeys.put(query.getId(), query.getKey());
                        return query;
                    }
            ).collect(Collectors.toList()));
        } else {
            finalTsKvQueryList = tsKvQueryList;
        }
        Map<EntityData, ListenableFuture<List<ReadTsKvQueryResult>>> fetchResultMap = new HashMap<>();
        List<EntityData> entityDataList = ctx.getData().getData();
        entityDataList.forEach(entityData -> fetchResultMap.put(entityData,
                tsService.findAllByQueries(ctx.getTenantId(), entityData.getEntityId(), finalTsKvQueryList)));
        return Futures.transform(Futures.allAsList(fetchResultMap.values()), f -> {
            Map<EntityData, Map<String, Long>> lastTsEntityMap = new HashMap<>();
            fetchResultMap.forEach((entityData, future) -> {
                try {
                    Map<String, Long> lastTsMap = new HashMap<>();
                    lastTsEntityMap.put(entityData, lastTsMap);

                    List<ReadTsKvQueryResult> queryResults = future.get();
                    if (queryResults != null) {
                        for (ReadTsKvQueryResult queryResult : queryResults) {
                            String queryKey = queriesKeys.get(queryResult.getQueryId());
                            if (queryKey != null) {
                                entityData.getTimeseries().merge(queryKey, queryResult.toTsValues(), ArrayUtils::addAll);
                                lastTsMap.merge(queryKey, queryResult.getLastEntryTs(), Math::max);
                            } else {
                                log.warn("ReadTsKvQueryResult for {} {} has queryId not matching the initial query",
                                        entityData.getEntityId().getEntityType(), entityData.getEntityId());
                            }
                        }
                    }
                    keys.forEach(key -> {
                        if (!entityData.getTimeseries().containsKey(key)) {
                            entityData.getTimeseries().put(key, new TsValue[0]);
                        }
                    });

                    if (cmd.isFetchLatestPreviousPoint()) {
                        entityData.getTimeseries().values().forEach(dataArray -> Arrays.sort(dataArray, (o1, o2) -> Long.compare(o2.getTs(), o1.getTs())));
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.warn("[{}][{}][{}] Failed to fetch historical data", ctx.getSessionId(), ctx.getCmdId(), entityData.getEntityId(), e);
                    ctx.sendWsMsg(new EntityDataUpdate(ctx.getCmdId(), SubscriptionErrorCode.INTERNAL_ERROR.getCode(), "Failed to fetch historical data!"));
                }
            });
            ctx.getWsLock().lock();
            try {
                EntityDataUpdate update;
                if (!ctx.isInitialDataSent()) {
                    update = new EntityDataUpdate(ctx.getCmdId(), ctx.getData(), null, ctx.getMaxEntitiesPerDataSubscription());
                    ctx.setInitialDataSent(true);
                } else {
                    update = new EntityDataUpdate(ctx.getCmdId(), null, entityDataList, ctx.getMaxEntitiesPerDataSubscription());
                }
                if (subscribe) {
                    ctx.createTimeSeriesSubscriptions(lastTsEntityMap, cmd.getStartTs(), cmd.getEndTs());
                }
                ctx.sendWsMsg(update);
                entityDataList.forEach(EntityData::clearTsAndAggData);
            } finally {
                ctx.getWsLock().unlock();
            }
            return ctx;
        }, wsCallBackExecutor);
    }

    /**
     * latest 列：混合存储（时序不在 SQL）时，{@code fetchData} 可能缺遥测 latest，先按实体补 {@code findLatest}，
     * 再在锁内建 latest 内部订阅并推包。SQL/Timescale 路径 latest 已在查询结果里，只建订阅并视情况发初始包。
     *
     * <p>已发过初始包时，增量里把 timeseries 置 null，避免和时序订阅抢着推空 map。
     */
    private void handleLatestCmd(TbEntityDataSubCtx ctx, LatestValueCmd latestCmd) {
        log.trace("[{}][{}] Going to process latest command: {}", ctx.getSessionId(), ctx.getCmdId(), latestCmd);
        // 混合模式：SQL 实体查询不含 Cassandra 里的 latest 遥测，缺的 key 再打一趟 tsService。
        if (!tsInSqlDB) {
            log.trace("[{}][{}] Going to fetch missing latest values: {}", ctx.getSessionId(), ctx.getCmdId(), latestCmd);
            List<String> allTsKeys = latestCmd.getKeys().stream()
                    .filter(key -> key.getType().equals(EntityKeyType.TIME_SERIES))
                    .map(EntityKey::getKey).collect(Collectors.toList());

            Map<EntityData, ListenableFuture<Map<String, TsValue>>> missingTelemetryFutures = new HashMap<>();
            for (EntityData entityData : ctx.getData().getData()) {
                Map<EntityKeyType, Map<String, TsValue>> latestEntityData = entityData.getLatest();
                Map<String, TsValue> tsEntityData = latestEntityData.get(EntityKeyType.TIME_SERIES);
                Set<String> missingTsKeys = new LinkedHashSet<>(allTsKeys);
                if (tsEntityData != null) {
                    missingTsKeys.removeAll(tsEntityData.keySet());
                } else {
                    tsEntityData = new HashMap<>();
                    latestEntityData.put(EntityKeyType.TIME_SERIES, tsEntityData);
                }

                ListenableFuture<List<TsKvEntry>> missingTsData = tsService.findLatest(ctx.getTenantId(), entityData.getEntityId(), missingTsKeys);
                missingTelemetryFutures.put(entityData, Futures.transform(missingTsData, this::toTsValue, MoreExecutors.directExecutor()));
            }
            Futures.addCallback(Futures.allAsList(missingTelemetryFutures.values()), new FutureCallback<>() {
                @Override
                public void onSuccess(@Nullable List<Map<String, TsValue>> result) {
                    missingTelemetryFutures.forEach((key, value) -> {
                        try {
                            key.getLatest().get(EntityKeyType.TIME_SERIES).putAll(value.get());
                        } catch (InterruptedException | ExecutionException e) {
                            log.warn("[{}][{}] Failed to lookup latest telemetry: {}:{}", ctx.getSessionId(), ctx.getCmdId(), key.getEntityId(), allTsKeys, e);
                        }
                    });
                    EntityDataUpdate update;
                    ctx.getWsLock().lock();
                    try {
                        ctx.createLatestValuesSubscriptions(latestCmd.getKeys());
                        if (!ctx.isInitialDataSent()) {
                            update = new EntityDataUpdate(ctx.getCmdId(), ctx.getData(), null, ctx.getMaxEntitiesPerDataSubscription());
                            ctx.setInitialDataSent(true);
                        } else {
                            // ctx 若同时订了时序，每次推完会清 timeseries；这里再带上空 map 会被前端当成「时序被清空」。
                            List<EntityData> preparedData = ctx.getData().getData().stream()
                                    .map(entityData -> new EntityData(entityData.getEntityId(), entityData.getLatest(), null))
                                    .toList();
                            update = new EntityDataUpdate(ctx.getCmdId(), null, preparedData, ctx.getMaxEntitiesPerDataSubscription());
                        }
                        ctx.sendWsMsg(update);
                    } finally {
                        ctx.getWsLock().unlock();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    log.warn("[{}][{}] Failed to process websocket command: {}:{}", ctx.getSessionId(), ctx.getCmdId(), ctx.getQuery(), latestCmd, t);
                    ctx.sendWsMsg(new EntityDataUpdate(ctx.getCmdId(), SubscriptionErrorCode.INTERNAL_ERROR.getCode(), "Failed to process websocket command!"));
                }
            }, wsCallBackExecutor);
        } else {
            ctx.getWsLock().lock();
            try {
                ctx.createLatestValuesSubscriptions(latestCmd.getKeys());
                checkAndSendInitialData(ctx);
            } finally {
                ctx.getWsLock().unlock();
            }
        }
    }

    /** {@code TsKvEntry} 列表转成 latest 列用的 {@code key → TsValue}。 */
    private Map<String, TsValue> toTsValue(List<TsKvEntry> data) {
        return data.stream().collect(Collectors.toMap(TsKvEntry::getKey, value -> new TsValue(value.getTs(), value.getValueAsString())));
    }

    /**
     * 取消一条 v2 命令：stop ctx（取消定时任务、清柜子里该 ctx 建的内部订阅），再从本表按 cmdId 移除。
     */
    @Override
    public void cancelSubscription(String sessionId, UnsubscribeCmd cmd) {
        cleanupAndCancel(getSubCtx(sessionId, cmd.getCmdId()));
    }

    /**
     * 停止 ctx 并从 {@link #subscriptionsBySessionId} 去掉该 cmdId。
     * ctx 为 null（从未订阅或已清）直接返回。不会 {@code remove} 整张会话 map，避免误删同会话其它命令。
     */
    private void cleanupAndCancel(TbAbstractSubCtx ctx) {
        if (ctx != null) {
            ctx.stop();
            if (ctx.getSessionId() != null) {
                Map<Integer, TbAbstractSubCtx> sessionSubs = subscriptionsBySessionId.get(ctx.getSessionId());
                if (sessionSubs != null) {
                    sessionSubs.remove(ctx.getCmdId());
                }
            }
        }
    }

    /**
     * 连接关闭时由 {@link org.thingsboard.server.service.ws.DefaultWebSocketService} 调用：
     * 整表摘掉该 sessionId，逐个 stop，清掉所有 v2 ctx 及其内部订阅。
     */
    @Override
    public void cancelAllSessionSubscriptions(String sessionId) {
        Map<Integer, TbAbstractSubCtx> sessionSubs = subscriptionsBySessionId.remove(sessionId);
        if (sessionSubs != null) {
            sessionSubs.values().forEach(sub -> {
                        try {
                            cleanupAndCancel(sub);
                        } catch (Exception e) {
                            log.warn("[{}] Failed to remove subscription {} due to ", sub.getTenantId(), sub, e);
                        }
                    }
            );
        }
    }

    /** 命令 limit 为 0 时用 {@link #DEFAULT_LIMIT}。 */
    private int getLimit(int limit) {
        return limit == 0 ? DEFAULT_LIMIT : limit;
    }

}
