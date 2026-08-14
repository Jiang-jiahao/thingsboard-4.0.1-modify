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
package org.thingsboard.server.service.edqs;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;
import org.thingsboard.server.edqs.util.EdqsRocksDb;

/**
 * 内存队列（{@code queue.type=in-memory}）场景下的 {@link EdqsSyncService} 实现。
 * <p>
 * 本地 EDQS 没有 Kafka events Topic 可探测，状态备份落在进程本地的
 * {@link EdqsRocksDb}。因此用「RocksDB 是否为新建空库」判断是否需要全量灌数：
 * <ul>
 *   <li>{@link EdqsRocksDb#isNew()} 为 {@code true}（首次启动、路径下无旧数据）
 *       → {@link #isSyncNeeded()} 为 {@code true}，应从 DB 全量写入事件队列，
 *       再由同进程 {@code LocalEdqsStateService} / {@code EdqsProcessor} 建内存索引
 *       并 {@code save} 回 RocksDB；</li>
 *   <li>已有 RocksDB 数据 → 视为上次已同步过，冷启动优先从 RocksDB 恢复，
 *       不必再扫全库（与 {@link KafkaEdqsSyncService} 看 topic 是否为空同理）。</li>
 * </ul>
 * <p>
 * 与 Kafka 实现一样：本类只实现 {@link #isSyncNeeded()}；真正扫库逻辑在父类
 * {@link EdqsSyncService#sync()}。是否执行 sync 还受 {@link DefaultEdqsService}
 * 中 {@code edqsSyncState} 约束。
 * <p>
 * <b>生效条件：</b>{@code queue.edqs.sync.enabled=true} 且 {@code queue.type=in-memory}
 *（典型为 monolith + {@code queue.edqs.mode=local}）。
 * 与 {@link KafkaEdqsSyncService} 互斥。
 *
 * @see EdqsSyncService
 * @see KafkaEdqsSyncService
 * @see EdqsRocksDb
 * @see DefaultEdqsService
 */
@Service
@RequiredArgsConstructor
@ConditionalOnExpression("'${queue.edqs.sync.enabled:true}' == 'true' && '${queue.type:null}' == 'in-memory'")
public class LocalEdqsSyncService extends EdqsSyncService {

    /**
     * 本地 EDQS 状态库；{@link #isSyncNeeded()} 委托其 {@link EdqsRocksDb#isNew()}。
     */
    private final EdqsRocksDb db;

    /**
     * RocksDB 为新建（空库）时需要全量同步。
     *
     * @return {@link EdqsRocksDb#isNew()} 的当前结果
     */
    @Override
    public boolean isSyncNeeded() {
        return db.isNew();
    }

}
