package com.jnks.iot.server.dao.sql.settings;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import com.jnks.iot.server.common.data.AdminSettings;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.TenantEntityDao;
import com.jnks.iot.server.dao.model.sql.AdminSettingsEntity;
import com.jnks.iot.server.dao.settings.AdminSettingsDao;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.UUID;

@Component
@SqlDao
@Slf4j
public class JpaAdminSettingsDao extends JpaAbstractDao<AdminSettingsEntity, AdminSettings> implements AdminSettingsDao, TenantEntityDao<AdminSettings> {

    @Autowired
    private AdminSettingsRepository adminSettingsRepository;

    @Override
    protected Class<AdminSettingsEntity> getEntityClass() {
        return AdminSettingsEntity.class;
    }

    @Override
    protected JpaRepository<AdminSettingsEntity, UUID> getRepository() {
        return adminSettingsRepository;
    }

    @Override
    public AdminSettings findByTenantIdAndKey(UUID tenantId, String key) {
        return DaoUtil.getData(adminSettingsRepository.findByTenantIdAndKey(tenantId, key));
    }

    @Override
    @Transactional
    public boolean removeByTenantIdAndKey(UUID tenantId, String key) {
        if (adminSettingsRepository.existsByTenantIdAndKey(tenantId, key)) {
            adminSettingsRepository.deleteByTenantIdAndKey(tenantId, key);
            return true;
        }
        return false;
    }

    @Override
    @Transactional
    public void removeByTenantId(UUID tenantId) {
        adminSettingsRepository.deleteByTenantId(tenantId);
    }

    @Override
    public PageData<AdminSettings> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(adminSettingsRepository.findByTenantId(tenantId.getId(), DaoUtil.toPageable(pageLink)));
    }

}
