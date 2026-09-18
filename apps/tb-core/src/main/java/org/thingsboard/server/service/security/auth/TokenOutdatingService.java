package org.thingsboard.server.service.security.auth;

import org.thingsboard.server.common.data.id.UserId;

/**
 * JWT / 会话作废判定。
 * <p>
 * 用户改密、禁用或主动登出后，签发早于作废时间戳的令牌视为过期。
 *
 * @see DefaultTokenOutdatingService
 */
public interface TokenOutdatingService {

    /**
     * 判断令牌是否已因用户或会话作废而失效。
     */
    boolean isOutdated(String token, UserId userId);

}
