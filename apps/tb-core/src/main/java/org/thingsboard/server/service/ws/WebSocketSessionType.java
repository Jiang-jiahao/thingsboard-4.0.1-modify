package org.thingsboard.server.service.ws;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Optional;

@NoArgsConstructor
@AllArgsConstructor
public enum WebSocketSessionType {
    GENERAL(),
    TELEMETRY("telemetry"), // deprecated
    NOTIFICATIONS("notifications"); // deprecated

    private String name;

    public static Optional<WebSocketSessionType> forName(String name) {
        return Arrays.stream(values())
                .filter(sessionType -> StringUtils.equals(sessionType.name, name))
                .findFirst();
    }

}
