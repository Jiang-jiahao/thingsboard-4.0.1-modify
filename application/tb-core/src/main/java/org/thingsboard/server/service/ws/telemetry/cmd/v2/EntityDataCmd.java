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
package org.thingsboard.server.service.ws.telemetry.cmd.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.thingsboard.server.common.data.query.EntityDataQuery;
import org.thingsboard.server.service.ws.WsCmdType;

/**
 * Dashboard WebSocket <b>v2 实体数据</b>命令：实体表、时序图等一次发给服务器的那包 JSON。
 *
 * <p>反序列化后由 {@link org.thingsboard.server.service.ws.DefaultWebSocketService}
 * 转到 {@link org.thingsboard.server.service.subscription.DefaultTbEntityDataSubscriptionService#handleCmd}。
 * 前端用 {@link #cmdId}（继承自 {@link DataCmd}）标识「这张表/这张图」；
 * 服务端一个 cmdId 对应一个 {@code TbEntityDataSubCtx}，真正盯遥测时再按实体拆到订阅柜子里。
 *
 * <h2>字段怎么组合</h2>
 * {@link #query} 决定<strong>订哪些实体</strong>（过滤、分页、要带哪些 latest 列）。
 * 五个子命令决定<strong>对这一页实体做什么</strong>，可以同时带多个，也可以这次只带 query、下次只带子命令：
 * <ul>
 *   <li>{@link #latestCmd}：订/推每个实体的当前属性或最新遥测（表格单元格）；</li>
 *   <li>{@link #tsCmd}：查一段时间窗口再订后续点（实时曲线）；</li>
 *   <li>{@link #historyCmd}：只查历史窗口，不订（切到 History 时间窗）；</li>
 *   <li>{@link #aggHistoryCmd}：窗口内聚合值，可带对比区间，不订；</li>
 *   <li>{@link #aggTsCmd}：窗口聚合后再订增量（聚合卡片的实时模式）。</li>
 * </ul>
 * 处理顺序：有 query 先 {@code fetchData}；历史/聚合先查完再推；然后才 {@code latestCmd}/{@code tsCmd} 进柜子。
 *
 * <h2>同一 cmdId 再发</h2>
 * 视为更新同一张表，而不是新命令。{@link #hasAnyCmd()} 为 true 时会先清掉该 ctx 里旧的实体级内部订阅再按新子命令重建。
 * {@code query == null} 时不重查实体列表，复用 ctx 里已有的本页数据（例如只改时间窗）。
 *
 * <p>Jackson 用全参构造；短参构造给旧前端（没有聚合子命令）用，聚合两项为 null。
 *
 * @see EntityDataQuery
 * @see EntityDataUpdate
 * @see org.thingsboard.server.service.subscription.TbEntityDataSubCtx
 */
public class EntityDataCmd extends DataCmd {

    /**
     * 实体查询：谁出现在这一页（entityFilter、keyFilters、分页、entityFields、latestValues）。
     * {@code pageLink.dynamic=true} 时服务端会定时重跑，刷新「页上有哪些行」。
     * 可为 null：本次不换名单，只对 ctx 里已有实体执行子命令。
     */
    @Getter
    private final EntityDataQuery query;

    /**
     * 一次性历史时序：keys + [startTs, endTs]，查完推 {@code EntityData.timeseries}，不往柜子订。
     */
    @Getter
    private final EntityHistoryCmd historyCmd;

    /**
     * 最新值：要盯的 {@link org.thingsboard.server.common.data.query.EntityKey} 列表（遥测或各 scope 属性）。
     * 为本页每个实体按 key 类型拆内部订阅，增量推进 latest 列。
     */
    @Getter
    private final LatestValueCmd latestCmd;

    /**
     * 实时时序：keys + startTs + timeWindow，先查窗口再订后续点。
     * {@code endTs} 由 startTs+timeWindow 算出，见 {@link TimeSeriesCmd#getEndTs()}。
     */
    @Getter
    private final TimeSeriesCmd tsCmd;

    /**
     * 聚合历史：每个 {@link AggKey} 在 [startTs, endTs] 上一块聚合值，可选 previous 窗口做对比。不订阅。
     */
    @Getter
    private final AggHistoryCmd aggHistoryCmd;

    /**
     * 聚合时序：每个 AggKey 在 [startTs, startTs+timeWindow] 上聚合，然后按最后 ts 订后续点（当 latest 推）。
     */
    @Getter
    private final AggTimeSeriesCmd aggTsCmd;

    /**
     * 无聚合子命令时的构造（兼容旧调用）。{@code aggHistoryCmd}/{@code aggTsCmd} 为 null。
     */
    public EntityDataCmd(int cmdId, EntityDataQuery query, EntityHistoryCmd historyCmd, LatestValueCmd latestCmd, TimeSeriesCmd tsCmd) {
        this(cmdId, query, historyCmd, latestCmd, tsCmd, null, null);
    }

    /**
     * Jackson 入口。JSON 里缺的子命令字段为 null，表示这次不做那一类操作。
     */
    @JsonCreator
    public EntityDataCmd(@JsonProperty("cmdId") int cmdId,
                         @JsonProperty("query") EntityDataQuery query,
                         @JsonProperty("historyCmd") EntityHistoryCmd historyCmd,
                         @JsonProperty("latestCmd") LatestValueCmd latestCmd,
                         @JsonProperty("tsCmd") TimeSeriesCmd tsCmd,
                         @JsonProperty("aggHistoryCmd") AggHistoryCmd aggHistoryCmd,
                         @JsonProperty("aggTsCmd") AggTimeSeriesCmd aggTsCmd) {
        super(cmdId);
        this.query = query;
        this.historyCmd = historyCmd;
        this.latestCmd = latestCmd;
        this.tsCmd = tsCmd;
        this.aggHistoryCmd = aggHistoryCmd;
        this.aggTsCmd = aggTsCmd;
    }

    /**
     * 是否带了「对实体做什么」的子命令（不含 query 本身）。
     * 已有 ctx 且本方法为 true 时，handleCmd 会先 {@code clearEntitySubscriptions}，避免旧内部订阅继续推。
     * 只有 query、没有子命令时返回 false：只刷新实体列表，保留已有遥测订阅。
     */
    @JsonIgnore
    public boolean hasAnyCmd() {
        return historyCmd != null || latestCmd != null || tsCmd != null || aggHistoryCmd != null || aggTsCmd != null;
    }

    @Override
    public WsCmdType getType() {
        return WsCmdType.ENTITY_DATA;
    }
}
