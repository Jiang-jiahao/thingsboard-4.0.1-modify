package com.jnks.iot.server.service.security.auth.jwt;

import lombok.RequiredArgsConstructor;
import org.springframework.security.authentication.*;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.security.Authority;
import com.jnks.iot.server.common.data.security.UserCredentials;
import com.jnks.iot.server.dao.customer.CustomerService;
import com.jnks.iot.server.dao.user.UserService;
import com.jnks.iot.server.service.security.auth.RefreshAuthenticationToken;
import com.jnks.iot.server.service.security.auth.TokenOutdatingService;
import com.jnks.iot.server.service.security.model.SecurityUser;
import com.jnks.iot.server.service.security.model.UserPrincipal;
import com.jnks.iot.server.service.security.model.token.JwtTokenFactory;
import com.jnks.iot.server.service.security.model.token.RawAccessJwtToken;

import java.util.UUID;

/**
 * 刷新token认证器
 */
@Component
@RequiredArgsConstructor
public class RefreshTokenAuthenticationProvider implements AuthenticationProvider {
    private final JwtTokenFactory tokenFactory;
    private final UserService userService;
    private final CustomerService customerService;
    private final TokenOutdatingService tokenOutdatingService;

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        Assert.notNull(authentication, "No authentication data provided");
        RawAccessJwtToken rawAccessToken = (RawAccessJwtToken) authentication.getCredentials();
        SecurityUser unsafeUser = tokenFactory.parseRefreshToken(rawAccessToken.getToken());
        UserPrincipal principal = unsafeUser.getUserPrincipal();

        SecurityUser securityUser;
        if (principal.getType() == UserPrincipal.Type.USER_NAME) {
            securityUser = authenticateByUserId(unsafeUser.getId());
        } else {
            securityUser = authenticateByPublicId(principal.getValue());
        }
        securityUser.setSessionId(unsafeUser.getSessionId());
        if (tokenOutdatingService.isOutdated(rawAccessToken.getToken(), securityUser.getId())) {
            throw new CredentialsExpiredException("Token is outdated");
        }

        return new RefreshAuthenticationToken(securityUser);
    }

    private SecurityUser authenticateByUserId(UserId userId) {
        TenantId systemId = TenantId.SYS_TENANT_ID;
        User user = userService.findUserById(systemId, userId);
        if (user == null) {
            throw new UsernameNotFoundException("User not found by refresh token");
        }

        UserCredentials userCredentials = userService.findUserCredentialsByUserId(systemId, user.getId());
        if (userCredentials == null) {
            throw new UsernameNotFoundException("User credentials not found");
        }

        if (!userCredentials.isEnabled()) {
            throw new DisabledException("User is not active");
        }

        if (user.getAuthority() == null) throw new InsufficientAuthenticationException("User has no authority assigned");

        UserPrincipal userPrincipal = new UserPrincipal(UserPrincipal.Type.USER_NAME, user.getEmail());
        SecurityUser securityUser = new SecurityUser(user, userCredentials.isEnabled(), userPrincipal);

        return securityUser;
    }

    private SecurityUser authenticateByPublicId(String publicId) {
        TenantId systemId = TenantId.SYS_TENANT_ID;
        CustomerId customerId;
        try {
            customerId = new CustomerId(UUID.fromString(publicId));
        } catch (Exception e) {
            throw new BadCredentialsException("Refresh token is not valid");
        }
        Customer publicCustomer = customerService.findCustomerById(systemId, customerId);
        if (publicCustomer == null) {
            throw new UsernameNotFoundException("Public entity not found by refresh token");
        }

        if (!publicCustomer.isPublic()) {
            throw new BadCredentialsException("Refresh token is not valid");
        }

        User user = new User(new UserId(EntityId.NULL_UUID));
        user.setTenantId(publicCustomer.getTenantId());
        user.setCustomerId(publicCustomer.getId());
        user.setEmail(publicId);
        user.setAuthority(Authority.CUSTOMER_USER);
        user.setFirstName("Public");
        user.setLastName("Public");

        UserPrincipal userPrincipal = new UserPrincipal(UserPrincipal.Type.PUBLIC_ID, publicId);

        SecurityUser securityUser = new SecurityUser(user, true, userPrincipal);

        return securityUser;
    }

    /**
     * 判断该种类型的令牌是否支持
     * @param authentication
     * @return
     */
    @Override
    public boolean supports(Class<?> authentication) {
        return (RefreshAuthenticationToken.class.isAssignableFrom(authentication));
    }
}
