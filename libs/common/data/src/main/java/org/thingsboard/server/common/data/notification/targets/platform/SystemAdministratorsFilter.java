package org.thingsboard.server.common.data.notification.targets.platform;

import lombok.Data;

@Data
public class SystemAdministratorsFilter implements UsersFilter {

    @Override
    public UsersFilterType getType() {
        return UsersFilterType.SYSTEM_ADMINISTRATORS;
    }

}
