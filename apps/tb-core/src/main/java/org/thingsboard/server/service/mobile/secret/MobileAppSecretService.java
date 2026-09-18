package org.thingsboard.server.service.mobile.secret;

import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.security.model.JwtPair;
import org.thingsboard.server.service.security.model.SecurityUser;

/**
 * 移动端一次性密钥服务接口：生成短时密钥并换取 JWT。
 */
public interface MobileAppSecretService {

    /** 为当前用户生成一次性移动端密钥。 */
    String generateMobileAppSecret(SecurityUser securityUser);

    /** 用密钥换取 JWT 对，过期或不存在则抛异常。 */
    JwtPair getJwtPair(String secret) throws ThingsboardException;

}
