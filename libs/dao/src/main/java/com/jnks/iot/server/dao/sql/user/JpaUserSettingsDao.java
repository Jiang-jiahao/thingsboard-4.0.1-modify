package com.jnks.iot.server.dao.sql.user;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.settings.UserSettings;
import com.jnks.iot.server.common.data.settings.UserSettingsCompositeKey;
import com.jnks.iot.server.common.data.settings.UserSettingsType;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.TenantEntityDao;
import com.jnks.iot.server.dao.model.sql.UserSettingsEntity;
import com.jnks.iot.server.dao.user.UserSettingsDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.List;

@Slf4j
@Component
@SqlDao
public class JpaUserSettingsDao implements UserSettingsDao, TenantEntityDao<UserSettings> {

    @Autowired
    private UserSettingsRepository userSettingsRepository;

    @Override
    public UserSettings save(TenantId tenantId, UserSettings userSettings) {
        log.trace("save [{}][{}]", tenantId, userSettings);
        return DaoUtil.getData(userSettingsRepository.save(new UserSettingsEntity(userSettings)));
    }

    @Override
    public UserSettings findById(TenantId tenantId, UserSettingsCompositeKey id) {
        return DaoUtil.getData(userSettingsRepository.findById(id));
    }

    @Override
    public void removeById(TenantId tenantId, UserSettingsCompositeKey id) {
        userSettingsRepository.deleteById(id);
    }

    @Override
    public void removeByUserId(TenantId tenantId, UserId userId) {
        userSettingsRepository.deleteByUserId(userId.getId());
    }

    @Override
    public List<UserSettings> findByTypeAndPath(TenantId tenantId, UserSettingsType type, String... path) {
        log.trace("findByTypeAndPath [{}][{}][{}]", tenantId, type, path);
        return DaoUtil.convertDataList(userSettingsRepository.findByTypeAndPathExisting(type.name(), path));
    }

    @Override
    public PageData<UserSettings> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(userSettingsRepository.findByTenantId(tenantId.getId(), DaoUtil.toPageable(pageLink)));
    }

}
