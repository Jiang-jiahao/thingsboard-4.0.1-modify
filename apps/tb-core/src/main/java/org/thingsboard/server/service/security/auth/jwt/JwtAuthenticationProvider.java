package org.thingsboard.server.service.security.auth.jwt;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.stereotype.Component;
import org.thingsboard.server.service.security.auth.JwtAuthenticationToken;
import org.thingsboard.server.service.security.auth.TokenOutdatingService;
import org.thingsboard.server.service.security.exception.JwtExpiredTokenException;
import org.thingsboard.server.service.security.model.SecurityUser;
import org.thingsboard.server.service.security.model.token.JwtTokenFactory;
import org.thingsboard.server.service.security.model.token.RawAccessJwtToken;

/**
 * jwt认证器
 */
@Component
@RequiredArgsConstructor
public class JwtAuthenticationProvider implements AuthenticationProvider {

    private final JwtTokenFactory tokenFactory;
    private final TokenOutdatingService tokenOutdatingService;

    /**
     * 进行认证
     * @param authentication 待认证token
     * @return 认证结果
     * @throws AuthenticationException 认证异常
     */
    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        RawAccessJwtToken rawAccessToken = (RawAccessJwtToken) authentication.getCredentials();
        SecurityUser securityUser = authenticate(rawAccessToken.getToken());
        return new JwtAuthenticationToken(securityUser);
    }

    public SecurityUser authenticate(String accessToken) throws AuthenticationException {
        if (StringUtils.isEmpty(accessToken)) {
            throw new BadCredentialsException("Token is invalid");
        }
        SecurityUser securityUser = tokenFactory.parseAccessJwtToken(accessToken);
        // 判断令牌是否过期
        if (tokenOutdatingService.isOutdated(accessToken, securityUser.getId())) {
            throw new JwtExpiredTokenException("Token is outdated");
        }
        return securityUser;
    }

    /**
     * 只支持处理JwtAuthenticationToken的认证
     * @param authentication 待认证token
     * @return 是否可以使用该provider进行认证
     */
    @Override
    public boolean supports(Class<?> authentication) {
        return (JwtAuthenticationToken.class.isAssignableFrom(authentication));
    }
}
