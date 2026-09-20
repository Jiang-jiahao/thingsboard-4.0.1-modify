package com.jnks.iot.server.dao.user;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.security.UserCredentials;
import com.jnks.iot.server.dao.Dao;

import java.util.UUID;

/**
 * The Interface UserCredentialsDao.
 */
public interface UserCredentialsDao extends Dao<UserCredentials> {

    /**
     * Save or update user credentials object
     *
     * @param userCredentials the user credentials object
     * @return saved user credentials object
     */
    UserCredentials save(TenantId tenantId, UserCredentials userCredentials);

    /**
     * Find user credentials by user id.
     *
     * @param userId the user id
     * @return the user credentials object
     */
    UserCredentials findByUserId(TenantId tenantId, UUID userId);

    /**
     * Find user credentials by activate token.
     *
     * @param activateToken the activate token
     * @return the user credentials object
     */
    UserCredentials findByActivateToken(TenantId tenantId, String activateToken);

    /**
     * Find user credentials by reset token.
     *
     * @param resetToken the reset token
     * @return the user credentials object
     */
    UserCredentials findByResetToken(TenantId tenantId, String resetToken);

    void removeByUserId(TenantId tenantId, UserId userId);

    void setLastLoginTs(TenantId tenantId, UserId userId, long lastLoginTs);

    int incrementFailedLoginAttempts(TenantId tenantId, UserId userId);

    void setFailedLoginAttempts(TenantId tenantId, UserId userId, int failedLoginAttempts);

}
