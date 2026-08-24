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
package org.thingsboard.server.service.edge.rpc.sync;

import com.google.common.util.concurrent.ListenableFuture;
import org.thingsboard.server.common.data.edge.Edge;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.gen.edge.v1.AttributesRequestMsg;
import org.thingsboard.server.gen.edge.v1.DeviceCredentialsRequestMsg;
import org.thingsboard.server.gen.edge.v1.EntityViewsRequestMsg;
import org.thingsboard.server.gen.edge.v1.RelationRequestMsg;
import org.thingsboard.server.gen.edge.v1.RuleChainMetadataRequestMsg;
import org.thingsboard.server.gen.edge.v1.UserCredentialsRequestMsg;
import org.thingsboard.server.gen.edge.v1.WidgetBundleTypesRequestMsg;

/**
 * Edge 向云端请求缺失数据时的同步处理契约。
 * <p>
 * 将请求转为 Edge 事件写入队列，再由 gRPC 会话下行推送。部分方法自 3.9.1 起废弃，将随旧协议移除。
 */
public interface EdgeRequestsService {

    /** 将规则链元数据请求转为 ADDED 事件推给该 Edge。 */
    @Deprecated(since = "3.9.1", forRemoval = true)
    ListenableFuture<Void> processRuleChainMetadataRequestMsg(TenantId tenantId, Edge edge, RuleChainMetadataRequestMsg ruleChainMetadataRequestMsg);

    /** 查询实体属性与最新时序，组装后作为 ATTRIBUTES_UPDATED / TIMESERIES_UPDATED 事件下发。 */
    ListenableFuture<Void> processAttributesRequestMsg(TenantId tenantId, Edge edge, AttributesRequestMsg attributesRequestMsg);

    /** 查询实体 FROM/TO 关系并作为 RELATION ADDED 事件下发（排除与 Edge 自身的关系）。 */
    ListenableFuture<Void> processRelationRequestMsg(TenantId tenantId, Edge edge, RelationRequestMsg relationRequestMsg);

    /** 将设备凭据更新请求转为 CREDENTIALS_UPDATED 事件。 */
    @Deprecated(since = "3.9.1", forRemoval = true)
    ListenableFuture<Void> processDeviceCredentialsRequestMsg(TenantId tenantId, Edge edge, DeviceCredentialsRequestMsg deviceCredentialsRequestMsg);

    /** 将用户凭据更新请求转为 CREDENTIALS_UPDATED 事件。 */
    @Deprecated(since = "3.9.1", forRemoval = true)
    ListenableFuture<Void> processUserCredentialsRequestMsg(TenantId tenantId, Edge edge, UserCredentialsRequestMsg userCredentialsRequestMsg);

    /** 将部件包下的部件类型作为 ADDED 事件下发。 */
    @Deprecated(since = "3.9.1", forRemoval = true)
    ListenableFuture<Void> processWidgetBundleTypesRequestMsg(TenantId tenantId, Edge edge, WidgetBundleTypesRequestMsg widgetBundleTypesRequestMsg);

    /** 查找实体关联的实体视图，仅下发已分配给该 Edge 的视图。 */
    @Deprecated(since = "3.9.1", forRemoval = true)
    ListenableFuture<Void> processEntityViewsRequestMsg(TenantId tenantId, Edge edge, EntityViewsRequestMsg entityViewsRequestMsg);
}
