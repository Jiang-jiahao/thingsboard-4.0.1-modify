package com.jnks.iot.server.service.security.auth.jwt.extractor;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.service.security.auth.constants.JnksIotSecurityConstants;

@Component(value="jwtHeaderTokenExtractor")
public class JwtHeaderTokenExtractor implements TokenExtractor {
    public static final String HEADER_PREFIX = "Bearer ";

    @Override
    public String extract(HttpServletRequest request) {
        String header = request.getHeader(JnksIotSecurityConstants.JWT_TOKEN_HEADER_PARAM);
        if (StringUtils.isBlank(header)) {
            header = request.getHeader(JnksIotSecurityConstants.JWT_TOKEN_HEADER_PARAM_V2);
            if (StringUtils.isBlank(header)) {
                throw new AuthenticationServiceException("Authorization header cannot be blank!");
            }
        }

        if (header.length() < HEADER_PREFIX.length()) {
            throw new AuthenticationServiceException("Invalid authorization header size.");
        }

        return header.substring(HEADER_PREFIX.length(), header.length());
    }
}
