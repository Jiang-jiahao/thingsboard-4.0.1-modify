package com.jnks.iot.server.dao.sql.user;

import lombok.RequiredArgsConstructor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.security.UserAuthSettings;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.TenantEntityDao;
import com.jnks.iot.server.dao.model.sql.UserAuthSettingsEntity;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.user.UserAuthSettingsDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.UUID;

@Component
@RequiredArgsConstructor
@SqlDao
public class JpaUserAuthSettingsDao extends JpaAbstractDao<UserAuthSettingsEntity, UserAuthSettings> implements UserAuthSettingsDao, TenantEntityDao<UserAuthSettings> {

    private final UserAuthSettingsRepository repository;

    @Override
    public UserAuthSettings findByUserId(UserId userId) {
        return DaoUtil.getData(repository.findByUserId(userId.getId()));
    }

    @Override
    public void removeByUserId(UserId userId) {
        repository.deleteByUserId(userId.getId());
    }

    @Override
    protected Class<UserAuthSettingsEntity> getEntityClass() {
        return UserAuthSettingsEntity.class;
    }

    @Override
    protected JpaRepository<UserAuthSettingsEntity, UUID> getRepository() {
        return repository;
    }

}
