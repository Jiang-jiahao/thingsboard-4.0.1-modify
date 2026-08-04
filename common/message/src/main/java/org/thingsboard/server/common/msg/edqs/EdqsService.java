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
package org.thingsboard.server.common.msg.edqs;

import org.thingsboard.server.common.data.ObjectType;
import org.thingsboard.server.common.data.edqs.EdqsObject;
import org.thingsboard.server.common.data.edqs.ToCoreEdqsMsg;
import org.thingsboard.server.common.data.edqs.ToCoreEdqsRequest;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;

/**
 * EDQS 写路径 / 系统协作接口：实体或关联数据变更后，通知 EDQS 更新索引。
 * <p>
 * 典型调用方：DAO（属性、时序、实体保存后的事件监听等）。
 * 真实现如 application/edqs 中的 {@code DefaultEdqsService}（转成事件发队列或本地更新）；
 * 无真实现时由 {@code DummyEdqsService} 空实现占位，避免 DAO 启动缺 Bean。
 * <p>
 * 与 {@link EdqsApiService} 的分工：本接口负责<strong>变更同步（写）</strong>；
 * {@link EdqsApiService} 负责<strong>实体查询（读）</strong>。
 */
public interface EdqsService {

    /**
     * 实体更新：按实体 ID 与领域对象通知 EDQS。
     */
    void onUpdate(TenantId tenantId, EntityId entityId, Object entity);

    /**
     * 对象更新：按对象类型与 {@link EdqsObject}（如属性 KV、关系等）通知 EDQS。
     */
    void onUpdate(TenantId tenantId, ObjectType objectType, EdqsObject object);

    /**
     * 实体删除。
     */
    void onDelete(TenantId tenantId, EntityId entityId);

    /**
     * 按对象类型删除（如删属性、关系）。
     */
    void onDelete(TenantId tenantId, ObjectType objectType, EdqsObject object);

    /**
     * 处理发往 Core 侧的 EDQS 系统请求（如同步控制类请求）。
     */
    void processSystemRequest(ToCoreEdqsRequest request);

    /**
     * 处理发往 Core 侧的 EDQS 系统消息。
     */
    void processSystemMsg(ToCoreEdqsMsg request);

}
