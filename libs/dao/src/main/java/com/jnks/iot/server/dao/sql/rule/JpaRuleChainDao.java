package com.jnks.iot.server.dao.sql.rule;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.edqs.fields.RuleChainFields;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.rule.RuleChain;
import com.jnks.iot.server.common.data.rule.RuleChainType;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.model.sql.RuleChainEntity;
import com.jnks.iot.server.dao.rule.RuleChainDao;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Component
@SqlDao
public class JpaRuleChainDao extends JpaAbstractDao<RuleChainEntity, RuleChain> implements RuleChainDao {

    @Autowired
    private RuleChainRepository ruleChainRepository;

    @Override
    protected Class<RuleChainEntity> getEntityClass() {
        return RuleChainEntity.class;
    }

    @Override
    protected JpaRepository<RuleChainEntity, UUID> getRepository() {
        return ruleChainRepository;
    }

    @Override
    public PageData<RuleChain> findRuleChainsByTenantId(UUID tenantId, PageLink pageLink) {
        log.debug("Try to find rule chains by tenantId [{}] and pageLink [{}]", tenantId, pageLink);
        return DaoUtil.toPageData(ruleChainRepository
                .findByTenantId(
                        tenantId,
                        pageLink.getTextSearch(),
                        DaoUtil.toPageable(pageLink)));
    }

    @Override
    public PageData<RuleChain> findRuleChainsByTenantIdAndType(UUID tenantId, RuleChainType type, PageLink pageLink) {
        log.debug("Try to find rule chains by tenantId [{}], type [{}] and pageLink [{}]", tenantId, type, pageLink);
        return DaoUtil.toPageData(ruleChainRepository
                .findByTenantIdAndType(
                        tenantId,
                        type,
                        pageLink.getTextSearch(),
                        DaoUtil.toPageable(pageLink)));
    }

    @Override
    public RuleChain findRootRuleChainByTenantIdAndType(UUID tenantId, RuleChainType type) {
        log.debug("Try to find root rule chain by tenantId [{}] and type [{}]", tenantId, type);
        return DaoUtil.getData(ruleChainRepository.findByTenantIdAndTypeAndRootIsTrue(tenantId, type));
    }

    @Override
    public Collection<RuleChain> findByTenantIdAndTypeAndName(TenantId tenantId, RuleChainType type, String name) {
        return DaoUtil.convertDataList(ruleChainRepository.findByTenantIdAndTypeAndName(tenantId.getId(), type, name));
    }

    @Override
    public Long countByTenantId(TenantId tenantId) {
        return ruleChainRepository.countByTenantId(tenantId.getId());
    }

    @Override
    public RuleChain findByTenantIdAndExternalId(UUID tenantId, UUID externalId) {
        return DaoUtil.getData(ruleChainRepository.findByTenantIdAndExternalId(tenantId, externalId));
    }

    @Override
    public PageData<RuleChain> findByTenantId(UUID tenantId, PageLink pageLink) {
        return findRuleChainsByTenantId(tenantId, pageLink);
    }

    @Override
    public RuleChainId getExternalIdByInternal(RuleChainId internalId) {
        return Optional.ofNullable(ruleChainRepository.getExternalIdById(internalId.getId()))
                .map(RuleChainId::new).orElse(null);
    }

    @Override
    public RuleChain findDefaultEntityByTenantId(UUID tenantId) {
        return findRootRuleChainByTenantIdAndType(tenantId, RuleChainType.CORE);
    }

    @Override
    public PageData<RuleChain> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return findRuleChainsByTenantId(tenantId.getId(), pageLink);
    }

    @Override
    public List<RuleChainFields> findNextBatch(UUID id, int batchSize) {
        return ruleChainRepository.findNextBatch(id, Limit.of(batchSize));
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.RULE_CHAIN;
    }

}
