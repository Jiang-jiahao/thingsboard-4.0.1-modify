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
