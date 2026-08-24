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
package org.thingsboard.server.service.edge;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.edge.Edge;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.rule.RuleChain;
import org.thingsboard.server.common.data.sync.ie.importing.csv.BulkImportColumnType;
import org.thingsboard.server.dao.edge.EdgeService;
import org.thingsboard.server.dao.rule.RuleChainService;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.entitiy.edge.TbEdgeService;
import org.thingsboard.server.service.security.model.SecurityUser;
import org.thingsboard.server.service.sync.ie.importing.csv.AbstractBulkImportService;

import java.util.Map;
import java.util.Optional;

/**
 * CSV Edge 批量导入实现。
 * <p>
 * 由导入相关 Controller 调用，映射名称、类型、路由密钥等字段后，取租户的 Edge 根规则链模板，
 * 再委托 {@link TbEdgeService#save} 落库（含审计日志与 Edge 相关副作用）。
 * 按名称查找已有 Edge 走 {@link EdgeService} DAO。
 *
 * @see AbstractBulkImportService
 * @see TbEdgeService
 */
@Service
@TbCoreComponent
@RequiredArgsConstructor
public class EdgeBulkImportService extends AbstractBulkImportService<Edge> {
    private final EdgeService edgeService;
    private final TbEdgeService tbEdgeService;
    private final RuleChainService ruleChainService;

    /** 将 CSV 列映射到 Edge 名称、类型、标签、描述、路由密钥与密钥。 */
    @Override
    protected void setEntityFields(Edge entity, Map<BulkImportColumnType, String> fields) {
        ObjectNode additionalInfo = getOrCreateAdditionalInfoObj(entity);
        fields.forEach((columnType, value) -> {
            switch (columnType) {
                case NAME:
                    entity.setName(value);
                    break;
                case TYPE:
                    entity.setType(value);
                    break;
                case LABEL:
                    entity.setLabel(value);
                    break;
                case DESCRIPTION:
                    additionalInfo.set("description", new TextNode(value));
                    break;
                case ROUTING_KEY:
                    entity.setRoutingKey(value);
                    break;
                case SECRET:
                    entity.setSecret(value);
                    break;
            }
        });
        entity.setAdditionalInfo(additionalInfo);
    }

    /** 绑定 Edge 根规则链模板后经 {@link TbEdgeService} 保存。 */
    @SneakyThrows
    @Override
    protected Edge saveEntity(SecurityUser user, Edge entity, Map<BulkImportColumnType, String> fields) {
        RuleChain edgeTemplateRootRuleChain = ruleChainService.getEdgeTemplateRootRuleChain(user.getTenantId());
        return tbEdgeService.save(entity, edgeTemplateRootRuleChain, user);
    }

    /** 按租户与名称查找 Edge，不存在则返回空对象供后续填充。 */
    @Override
    protected Edge findOrCreateEntity(TenantId tenantId, String name) {
        return Optional.ofNullable(edgeService.findEdgeByTenantIdAndName(tenantId, name))
                .orElseGet(Edge::new);
    }

    /** 将导入用户的租户与客户写到 Edge 上。 */
    @Override
    protected void setOwners(Edge entity, SecurityUser user) {
        entity.setTenantId(user.getTenantId());
        entity.setCustomerId(user.getCustomerId());
    }

    /** 声明本导入器处理的实体类型为 Edge。 */
    @Override
    protected EntityType getEntityType() {
        return EntityType.EDGE;
    }

}
