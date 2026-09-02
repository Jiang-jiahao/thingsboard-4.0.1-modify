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

import lombok.Data;
import org.thingsboard.server.common.data.kv.Aggregation;

/**
 * 聚合卡片上的一列，比如「本月用电量」。
 *
 * <p>不是把每天的点都返回，而是把一段时间收成<strong>一个数</strong>（总和、平均等）。
 * 卡片若显示「1200，比上月 +22%」，1200 是当前这段算出来的，上月那个数才用下面的 previous 三个字段。
 *
 * <p>用在 {@link AggHistoryCmd}（历史，可对比）和 {@link AggTimeSeriesCmd}（实时，没有上一周期）。
 */
@Data
public class AggKey {

    /**
     * 这一列在前端的编号，用来把结果塞回对应卡片。
     * 不是遥测名：用电量 SUM 和用电量 AVG 可以是两个 id、同一个 {@link #key}。
     */
    private int id;

    /** 遥测叫什么，例如 energy、temperature。 */
    private String key;

    /** 怎么收成一个数：SUM 求和、AVG 平均、MAX / MIN / COUNT 等。 */
    private Aggregation agg;

    /**
     * 「拿来比的那段」从哪天开始。命令上的 startTs 是「你正在看的这段」（比如本月）；
     * 这里是再往前一段（比如上月）。没开对比则为 null，不查上一周期。
     */
    private Long previousStartTs;

    /** 「拿来比的那段」到哪天结束。一般接到当前段的 startTs，例如上月最后一天。 */
    private Long previousEndTs;

    /**
     * true：这列不要本月的数，只要上月的数（卡片设置成「显示上一周期」）。
     * false / null：照常算当前这段。实时聚合命令用不到这个字段。
     */
    private Boolean previousValueOnly;

}
