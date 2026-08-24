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
package org.thingsboard.server.service.entitiy.mobile;

import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.id.OAuth2ClientId;
import org.thingsboard.server.common.data.mobile.bundle.MobileAppBundle;

import java.util.List;

/**
 * 移动应用包业务层契约：保存、绑定 OAuth2 客户端与删除。
 */
public interface TbMobileAppBundleService {

    /** 保存移动应用包并可同时绑定 OAuth2 客户端。 */
    MobileAppBundle save(MobileAppBundle mobileAppBundle, List<OAuth2ClientId> oauth2Clients, User user) throws Exception;

    /** 更新移动应用包绑定的 OAuth2 客户端列表。 */
    void updateOauth2Clients(MobileAppBundle mobileAppBundle, List<OAuth2ClientId> oAuth2ClientIds, User user);

    /** 删除移动应用包。 */
    void delete(MobileAppBundle mobileAppBundle, User user);

}
