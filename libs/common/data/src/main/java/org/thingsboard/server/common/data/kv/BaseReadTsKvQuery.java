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
package org.thingsboard.server.common.data.kv;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.ZoneId;

/**
 * 去时序库读数据时用的查询条件：读哪个遥测、哪段时间、要不要收成更少的点。
 *
 * <p>父类已经带了三样：{@code key}（遥测名）、{@code startTs}～{@code endTs}（从哪到哪）。
 * 这里多出来的是「怎么读」：
 * <ul>
 *   <li>原样返回每一个点（曲线），还是按时间切片收成 SUM/AVG（聚合卡片、降采样曲线）；</li>
 *   <li>最多返回几个点；</li>
 *   <li>时间正序还是倒序。</li>
 * </ul>
 *
 * <p>聚合卡片会把 interval 设成整段窗口那么长、limit=1，于是整月用电只得到一个数。
 * 实时曲线则 interval=1 分钟、limit=500，得到一串点。
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class BaseReadTsKvQuery extends BaseTsKvQuery implements ReadTsKvQuery {

    /**
     * 要不要聚合、按多长切一刀。
     * NONE 表示不要切，把原始点都拿回来；否则按 interval 切桶，每桶一个 SUM/AVG。
     */
    private final AggregationParams aggParameters;

    /** 最多返回几个点。聚合卡片通常是 1；曲线会设一个上限防止一次拉太多。 */
    private final int limit;

    /** 按时间排序：DESC 最新的在前，ASC 从早到晚。默认 DESC。 */
    private final String order;

    /** 按固定毫秒间隔聚合。order 默认倒序。 */
    public BaseReadTsKvQuery(String key, long startTs, long endTs, long interval, int limit, Aggregation aggregation) {
        this(key, startTs, endTs, interval, limit, aggregation, "DESC");
    }

    /** 同上，可指定正序/倒序。聚合历史用的就是这个：interval=整段长度，limit=1。 */
    public BaseReadTsKvQuery(String key, long startTs, long endTs, long interval, int limit, Aggregation aggregation, String descOrder) {
        this(key, startTs, endTs, AggregationParams.of(aggregation, IntervalType.MILLISECONDS, ZoneId.systemDefault(), interval), limit, descOrder);
    }

    /** 聚合方式已经打成 {@link AggregationParams}（也可以按自然月/周切，不只是固定毫秒）。 */
    public BaseReadTsKvQuery(String key, long startTs, long endTs, AggregationParams parameters, int limit) {
        this(key, startTs, endTs, parameters, limit, "DESC");
    }

    /** 最完整的构造：时间范围 + 怎么聚合 + 条数 + 排序。 */
    public BaseReadTsKvQuery(String key, long startTs, long endTs, AggregationParams parameters, int limit, String order) {
        super(key, startTs, endTs);
        this.aggParameters = parameters;
        this.limit = limit;
        this.order = order;
    }

    /** 只给时间和 key：整段窗口平均成 1 个数。 */
    public BaseReadTsKvQuery(String key, long startTs, long endTs) {
        this(key, startTs, endTs, AggregationParams.milliseconds(Aggregation.AVG, endTs - startTs), 1, "DESC");
    }

    /** 不聚合，只要原始点（Aggregation.NONE）。 */
    public BaseReadTsKvQuery(String key, long startTs, long endTs, int limit, String order) {
        this(key, startTs, endTs, AggregationParams.none(), limit, order);
    }

    /** 复制一条查询，只换时间范围（其它条件不变）。 */
    public BaseReadTsKvQuery(ReadTsKvQuery query, long startTs, long endTs) {
        super(query.getId(), query.getKey(), startTs, endTs);
        this.aggParameters = query.getAggParameters();
        this.limit = query.getLimit();
        this.order = query.getOrder();
    }
}
