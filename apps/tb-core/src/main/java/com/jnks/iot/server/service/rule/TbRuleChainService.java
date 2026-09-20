package com.jnks.iot.server.service.rule;

import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.rule.*;
import com.jnks.iot.server.service.entitiy.SimpleTbEntityService;

import java.util.List;
import java.util.Set;

/**
 * Core 侧规则链实体服务。
 * <p>
 * 在 REST 层封装规则链 CRUD、元数据保存、根链设置，以及 Output 节点标签变更后的关联规则链更新。
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
    RuleChain setRootRuleChain(TenantId tenantId, RuleChain ruleChain, User user) throws JnksIotException;

    /**
     * 保存规则链节点与连线元数据，可选同步关联规则链。
     */
    RuleChainMetaData saveRuleChainMetaData(TenantId tenantId, RuleChain ruleChain, RuleChainMetaData ruleChainMetaData,
                                            boolean updateRelated, User user) throws Exception;

    /**
     * 按组件定义版本升级规则节点配置 JSON。
     */
    RuleNode updateRuleNodeConfiguration(RuleNode ruleNode);
}
