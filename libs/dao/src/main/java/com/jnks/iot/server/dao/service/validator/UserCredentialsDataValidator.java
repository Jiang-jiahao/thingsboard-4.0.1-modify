package com.jnks.iot.server.dao.service.validator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.security.UserCredentials;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.exception.IncorrectParameterException;
import com.jnks.iot.server.dao.service.DataValidator;
import com.jnks.iot.server.dao.user.UserCredentialsDao;
import com.jnks.iot.server.dao.user.UserService;

@Component
public class UserCredentialsDataValidator extends DataValidator<UserCredentials> {

    @Autowired
    private UserCredentialsDao userCredentialsDao;

    @Autowired
    @Lazy
    private UserService userService;

    @Override
    protected void validateCreate(TenantId tenantId, UserCredentials userCredentials) {
        throw new IncorrectParameterException("Creation of new user credentials is prohibited.");
    }

    @Override
    protected void validateDataImpl(TenantId tenantId, UserCredentials userCredentials) {
        if (userCredentials.getUserId() == null) {
            throw new DataValidationException("User credentials should be assigned to user!");
        }
        if (userCredentials.isEnabled()) {
            if (StringUtils.isEmpty(userCredentials.getPassword())) {
                throw new DataValidationException("Enabled user credentials should have password!");
            }
            if (StringUtils.isNotEmpty(userCredentials.getActivateToken())) {
                throw new DataValidationException("Enabled user credentials can't have activate token!");
            }
        }
        UserCredentials existingUserCredentialsEntity = userCredentialsDao.findById(tenantId, userCredentials.getId().getId());
        if (existingUserCredentialsEntity == null) {
            throw new DataValidationException("Unable to update non-existent user credentials!");
        }
        User user = userService.findUserById(tenantId, userCredentials.getUserId());
        if (user == null) {
            throw new DataValidationException("Can't assign user credentials to non-existent user!");
        }
    }
}
