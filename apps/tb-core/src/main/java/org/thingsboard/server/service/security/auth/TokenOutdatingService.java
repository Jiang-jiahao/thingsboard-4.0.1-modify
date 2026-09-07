/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
