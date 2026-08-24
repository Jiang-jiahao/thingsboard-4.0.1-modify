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
package org.thingsboard.server.service.rule;

import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.edge.Edge;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.RuleChainId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.rule.*;
import org.thingsboard.server.service.entitiy.SimpleTbEntityService;

import java.util.List;
import java.util.Set;

/**
 * Core 侧规则链实体服务。
 * <p>
 * 在 REST 层封装规则链 CRUD、元数据保存、根链/Edge 模板设置，以及 Output 节点标签变更后的关联规则链更新。
 *
 * @see DefaultTbRuleChainService
 */
public interface TbRuleChainService extends SimpleTbEntityService<RuleChain> {

    /**
     * 收集规则链中所有 Output 节点名称作为输出标签。
     */
    Set<String> getRuleChainOutputLabels(TenantId tenantId, RuleChainId ruleChainId);

    /**
     * 查询引用本规则链的 Input 节点及其使用的输出标签。
     */
    List<RuleChainOutputLabelsUsage> getOutputLabelUsage(TenantId tenantId, RuleChainId ruleChainId);

    /**
     * 本链 Output 标签重命名后，同步更新关联规则链中的连线类型。
     */
    List<RuleChain> updateRelatedRuleChains(TenantId tenantId, RuleChainId ruleChainId, RuleChainUpdateResult result);

    /**
     * 按内置脚本模板创建默认规则链。
     */
    RuleChain saveDefaultByName(TenantId tenantId, DefaultRuleChainCreateRequest request, User user) throws Exception;

    /**
     * 将指定规则链设为租户根规则链。
     */
    RuleChain setRootRuleChain(TenantId tenantId, RuleChain ruleChain, User user) throws ThingsboardException;

    /**
     * 保存规则链节点与连线元数据，可选同步关联规则链。
     */
    RuleChainMetaData saveRuleChainMetaData(TenantId tenantId, RuleChain ruleChain, RuleChainMetaData ruleChainMetaData,
                                            boolean updateRelated, User user) throws Exception;

    /**
     * 将规则链分配到 Edge。
     */
    RuleChain assignRuleChainToEdge(TenantId tenantId, RuleChain ruleChain, Edge edge, User user) throws ThingsboardException;

    /**
     * 取消规则链与 Edge 的分配。
     */
    RuleChain unassignRuleChainFromEdge(TenantId tenantId, RuleChain ruleChain, Edge edge, User user) throws ThingsboardException;

    /**
     * 设为 Edge 模板根规则链。
     */
    RuleChain setEdgeTemplateRootRuleChain(TenantId tenantId, RuleChain ruleChain, User user) throws ThingsboardException;

    /**
     * 标记新建 Edge 时自动分配该规则链。
     */
    RuleChain setAutoAssignToEdgeRuleChain(TenantId tenantId, RuleChain ruleChain, User user) throws ThingsboardException;

    /**
     * 取消新建 Edge 自动分配。
     */
    RuleChain unsetAutoAssignToEdgeRuleChain(TenantId tenantId, RuleChain ruleChain, User user) throws ThingsboardException;

    /**
     * 按组件定义版本升级规则节点配置 JSON。
     */
    RuleNode updateRuleNodeConfiguration(RuleNode ruleNode);
}
