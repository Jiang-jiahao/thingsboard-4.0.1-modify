package com.jnks.iot.server.dao.sql.user;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.security.UserCredentials;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.TenantEntityDao;
import com.jnks.iot.server.dao.model.sql.UserCredentialsEntity;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.user.UserCredentialsDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.UUID;

/**
 * Created by Valerii Sosliuk on 4/22/2017.
 */
@Component
@SqlDao
public class JpaUserCredentialsDao extends JpaAbstractDao<UserCredentialsEntity, UserCredentials> implements UserCredentialsDao, TenantEntityDao<UserCredentials> {

    @Autowired
    private UserCredentialsRepository userCredentialsRepository;

    @Override
    protected Class<UserCredentialsEntity> getEntityClass() {
        return UserCredentialsEntity.class;
    }

    @Override
    protected JpaRepository<UserCredentialsEntity, UUID> getRepository() {
        return userCredentialsRepository;
    }

    @Override
    public UserCredentials findByUserId(TenantId tenantId, UUID userId) {
        return DaoUtil.getData(userCredentialsRepository.findByUserId(userId));
    }

    @Override
    public UserCredentials findByActivateToken(TenantId tenantId, String activateToken) {
        return DaoUtil.getData(userCredentialsRepository.findByActivateToken(activateToken));
    }

    @Override
    public UserCredentials findByResetToken(TenantId tenantId, String resetToken) {
        return DaoUtil.getData(userCredentialsRepository.findByResetToken(resetToken));
    }

    @Override
    public void removeByUserId(TenantId tenantId, UserId userId) {
        userCredentialsRepository.removeByUserId(userId.getId());
    }

    @Override
    public void setLastLoginTs(TenantId tenantId, UserId userId, long lastLoginTs) {
        userCredentialsRepository.updateLastLoginTsByUserId(userId.getId(), lastLoginTs);
    }

    @Override
    public int incrementFailedLoginAttempts(TenantId tenantId, UserId userId) {
        return userCredentialsRepository.incrementFailedLoginAttemptsByUserId(userId.getId());
    }

    @Override
    public void setFailedLoginAttempts(TenantId tenantId, UserId userId, int failedLoginAttempts) {
        userCredentialsRepository.updateFailedLoginAttemptsByUserId(userId.getId(), failedLoginAttempts);
    }

    @Override
    public PageData<UserCredentials> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(userCredentialsRepository.findByTenantId(tenantId.getId(), DaoUtil.toPageable(pageLink)));
    }

}
