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
package org.thingsboard.server.service.entitiy.domain;

import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.domain.Domain;
import org.thingsboard.server.common.data.id.OAuth2ClientId;

import java.util.List;

/**
 * 登录域名（Domain）业务层契约：保存、绑定 OAuth2 客户端与删除。
 */
public interface TbDomainService {

    /** 保存域名并可同时绑定 OAuth2 客户端。 */
    Domain save(Domain domain, List<OAuth2ClientId> oAuth2Clients, User user) throws Exception;

    /** 更新域名绑定的 OAuth2 客户端列表。 */
    void updateOauth2Clients(Domain domain, List<OAuth2ClientId> oAuth2ClientIds, User user);

    /** 删除域名。 */
    void delete(Domain domain, User user);

}
