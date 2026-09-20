package com.jnks.iot.server.service.sync.ie.exporting.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.rule.RuleChain;
import com.jnks.iot.server.common.data.rule.RuleChainMetaData;
import com.jnks.iot.server.common.data.sync.ie.RuleChainExportData;
import com.jnks.iot.server.dao.rule.RuleChainService;
import com.jnks.iot.server.service.sync.vc.data.EntitiesExportCtx;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static com.jnks.iot.server.service.sync.ie.importing.impl.RuleChainImportService.PROCESSED_CONFIG_FIELDS_PATTERN;

/**
 * 针对 {@link RuleChain} 的导出服务，继承 {@link BaseEntityExportService}。
 * <p>
 * 导出完整规则链元数据：节点 ID 换成 externalId，递归替换节点配置中的实体 UUID，
 * 规则链连接的目标链也替换为 externalId。
 */
@Service
@RequiredArgsConstructor
public class RuleChainExportService extends BaseEntityExportService<RuleChainId, RuleChain, RuleChainExportData> {

    private final RuleChainService ruleChainService;

    /**
     * 导出节点与连接：节点 ID 及配置内 UUID、目标规则链均替换为 externalId。
     */
    @Override
    protected void setRelatedEntities(EntitiesExportCtx<?> ctx, RuleChain ruleChain, RuleChainExportData exportData) {
        RuleChainMetaData metaData = ruleChainService.loadRuleChainMetaData(ctx.getTenantId(), ruleChain.getId());
        Optional.ofNullable(metaData.getNodes()).orElse(Collections.emptyList())
                .forEach(ruleNode -> {
                    ruleNode.setRuleChainId(null);
                    ctx.putExternalId(ruleNode.getId(), ruleNode.getExternalId());
                    ruleNode.setId(ctx.getExternalId(ruleNode.getId()));
                    ruleNode.setCreatedTime(0);
                    ruleNode.setExternalId(null);
                    replaceUuidsRecursively(ctx, ruleNode.getConfiguration(), Collections.emptySet(), PROCESSED_CONFIG_FIELDS_PATTERN);
                });
        Optional.ofNullable(metaData.getRuleChainConnections()).orElse(Collections.emptyList())
                .forEach(ruleChainConnectionInfo -> {
                    ruleChainConnectionInfo.setTargetRuleChainId(getExternalIdOrElseInternal(ctx, ruleChainConnectionInfo.getTargetRuleChainId()));
                });
        exportData.setMetaData(metaData);
        if (ruleChain.getFirstRuleNodeId() != null) {
            ruleChain.setFirstRuleNodeId(ctx.getExternalId(ruleChain.getFirstRuleNodeId()));
        }
    }

    @Override
    protected RuleChainExportData newExportData() {
        return new RuleChainExportData();
    }

    @Override
    public Set<EntityType> getSupportedEntityTypes() {
        return Set.of(EntityType.RULE_CHAIN);
    }

}
