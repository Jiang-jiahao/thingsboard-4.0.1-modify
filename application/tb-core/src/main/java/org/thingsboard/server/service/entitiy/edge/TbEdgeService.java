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
package org.thingsboard.server.service.entitiy.edge;

import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.edge.Edge;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.EdgeId;
import org.thingsboard.server.common.data.id.RuleChainId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.rule.RuleChain;

/**
 * Edge 业务层契约：保存/删除、分配客户、设置根规则链。
 * <p>
 * 由 EdgeController 调用；实现类委托 Edge DAO，新建时绑定规则链并写审计日志。
 */
public interface TbEdgeService {

    /** 保存 Edge；新建时绑定模板根规则链。 */
    Edge save(Edge edge, RuleChain edgeTemplateRootRuleChain, User user) throws Exception;

    /** 删除 Edge。 */
    void delete(Edge edge, User user);

    /** 将 Edge 分配给客户。 */
    Edge assignEdgeToCustomer(TenantId tenantId, EdgeId edgeId, Customer customer, User user) throws ThingsboardException;

    /** 取消 Edge 与客户的分配。 */
    Edge unassignEdgeFromCustomer(Edge edge, Customer customer, User user) throws ThingsboardException;

    /** 将 Edge 分配给公开客户。 */
    Edge assignEdgeToPublicCustomer(TenantId tenantId, EdgeId edgeId, User user) throws ThingsboardException;

    /** 设置 Edge 的根规则链。 */
    Edge setEdgeRootRuleChain(Edge edge, RuleChainId ruleChainId, User user) throws Exception;

}
