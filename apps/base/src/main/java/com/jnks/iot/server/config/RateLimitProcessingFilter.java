package com.jnks.iot.server.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import com.jnks.iot.server.cache.limits.RateLimitService;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.exception.TenantProfileNotFoundException;
import com.jnks.iot.server.common.data.limit.LimitedApi;
import com.jnks.iot.server.common.msg.tools.TbRateLimitsException;
import com.jnks.iot.server.exception.JnksIotErrorResponseHandler;
import com.jnks.iot.server.service.security.model.SecurityUser;

import java.io.IOException;

@Slf4j
@Component
@RequiredArgsConstructor
public class RateLimitProcessingFilter extends OncePerRequestFilter {

    private final JnksIotErrorResponseHandler errorResponseHandler;
    private final RateLimitService rateLimitService;

    @Override
    public void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws IOException, ServletException {
        SecurityUser user = getCurrentUser();
        if (user != null && !user.isSystemAdmin()) {
            try {
                // 如果不是系统用户，则检查租户的限流
                if (!rateLimitService.checkRateLimit(LimitedApi.REST_REQUESTS_PER_TENANT, user.getTenantId())) {
                    rateLimitExceeded(EntityType.TENANT, response);
                    return;
                }
            } catch (TenantProfileNotFoundException e) {
                log.debug("[{}] Failed to lookup tenant profile", user.getTenantId());
                errorResponseHandler.handle(new BadCredentialsException("Failed to lookup tenant profile"), response);
                return;
            }

            if (user.isCustomerUser()) {
                if (!rateLimitService.checkRateLimit(LimitedApi.REST_REQUESTS_PER_CUSTOMER, user.getTenantId(), user.getCustomerId())) {
                    rateLimitExceeded(EntityType.CUSTOMER, response);
                    return;
                }
            }
        }
        chain.doFilter(request, response);
    }

    @Override
    protected boolean shouldNotFilterAsyncDispatch() {
        return false;
    }

    @Override
    protected boolean shouldNotFilterErrorDispatch() {
        return false;
    }

    private void rateLimitExceeded(EntityType type, HttpServletResponse response) {
        errorResponseHandler.handle(new TbRateLimitsException(type), response);
    }

    protected SecurityUser getCurrentUser() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null && authentication.getPrincipal() instanceof SecurityUser) {
            return (SecurityUser) authentication.getPrincipal();
        } else {
            return null;
        }
    }

}
