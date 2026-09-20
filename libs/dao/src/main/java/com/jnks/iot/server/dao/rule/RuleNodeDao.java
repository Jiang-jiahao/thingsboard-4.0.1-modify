package com.jnks.iot.server.dao.rule;

import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.rule.RuleNode;
import com.jnks.iot.server.dao.Dao;

import java.util.List;

/**
 * Created by igor on 3/12/18.
 */
public interface RuleNodeDao extends Dao<RuleNode> {

    List<RuleNode> findRuleNodesByTenantIdAndType(TenantId tenantId, String type, String configurationSearch);

    PageData<RuleNode> findAllRuleNodesByType(String type, PageLink pageLink);

    PageData<RuleNode> findAllRuleNodesByTypeAndVersionLessThan(String type, int version, PageLink pageLink);

    PageData<RuleNodeId> findAllRuleNodeIdsByTypeAndVersionLessThan(String type, int version, PageLink pageLink);

    List<RuleNode> findAllRuleNodeByIds(List<RuleNodeId> ruleNodeIds);

    List<RuleNode> findByExternalIds(RuleChainId ruleChainId, List<RuleNodeId> externalIds);

    void deleteByIdIn(List<RuleNodeId> ruleNodeIds);

}
