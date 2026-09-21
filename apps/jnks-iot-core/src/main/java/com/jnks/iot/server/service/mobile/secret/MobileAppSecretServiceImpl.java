package com.jnks.iot.server.service.mobile.secret;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.event.TransactionalEventListener;
import com.jnks.iot.server.cache.JnksIotCacheValueWrapper;
import com.jnks.iot.server.cache.mobile.secret.MobileSecretEvictEvent;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.exception.JnksIotErrorCode;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.security.model.JwtPair;
import com.jnks.iot.server.dao.entity.AbstractCachedService;
import com.jnks.iot.server.dao.settings.SecuritySettingsService;
import com.jnks.iot.server.service.security.model.SecurityUser;
import com.jnks.iot.server.service.security.model.token.JwtTokenFactory;

import static com.jnks.iot.server.dao.settings.DefaultSecuritySettingsService.DEFAULT_MOBILE_SECRET_KEY_LENGTH;


/**
 * 移动端一次性密钥服务：生成安全随机密钥，缓存对应 JWT 对，供 App 换票。
 * <p>
 * <b>职责：</b>密钥长度取自安全设置；密钥→JWT 写入缓存；过期后无法换票。
 * <p>
 * <b>触发方式：</b>移动端登录/授权 API 调用；缓存驱逐事件监听。
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class MobileAppSecretServiceImpl extends AbstractCachedService<String, JwtPair, MobileSecretEvictEvent> implements MobileAppSecretService {

    private final JwtTokenFactory tokenFactory;
    private final SecuritySettingsService securitySettingsService;

    /** 生成一次性密钥并缓存 JWT 对。 */
    @Override
    public String generateMobileAppSecret(SecurityUser securityUser) {
        log.trace("Executing generateSecret for user [{}]", securityUser.getId());
        Integer mobileSecretKeyLength = securitySettingsService.getSecuritySettings().getMobileSecretKeyLength();
        String secret = StringUtils.generateSafeToken(mobileSecretKeyLength == null ? DEFAULT_MOBILE_SECRET_KEY_LENGTH : mobileSecretKeyLength);
        cache.put(secret, tokenFactory.createTokenPair(securityUser));
        return secret;
    }

    /** 按密钥取出 JWT 对，不存在或已过期则抛异常。 */
    @Override
    public JwtPair getJwtPair(String secret) throws JnksIotException {
        JnksIotCacheValueWrapper<JwtPair> jwtPair = cache.get(secret);
        if (jwtPair != null) {
            return jwtPair.get();
        } else {
            throw new JnksIotException("Jwt token not found or expired!", JnksIotErrorCode.JWT_TOKEN_EXPIRED);
        }
    }

    /** 处理密钥缓存驱逐事件。 */
    @TransactionalEventListener(classes = MobileSecretEvictEvent.class)
    @Override
    public void handleEvictEvent(MobileSecretEvictEvent event) {
        cache.evict(event.getSecret());
    }

}
