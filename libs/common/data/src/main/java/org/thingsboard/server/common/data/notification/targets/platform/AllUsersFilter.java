package org.thingsboard.server.common.data.notification.targets.platform;

import lombok.Data;

@Data
public class AllUsersFilter implements UsersFilter {

    @Override
    public UsersFilterType getType() {
        return UsersFilterType.ALL_USERS;
    }

}
