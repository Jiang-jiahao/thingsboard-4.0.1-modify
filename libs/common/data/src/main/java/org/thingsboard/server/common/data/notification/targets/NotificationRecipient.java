package org.thingsboard.server.common.data.notification.targets;

public interface NotificationRecipient {

    Object getId();

    String getTitle();

    default String getFirstName() {
        return null;
    }

    default String getLastName() {
        return null;
    }

    default String getEmail() {
        return null;
    }

}
