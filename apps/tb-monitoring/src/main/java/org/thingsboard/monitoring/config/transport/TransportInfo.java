package org.thingsboard.monitoring.config.transport;

import lombok.Data;

@Data
public class TransportInfo {

    private final TransportType transportType;
    private final String baseUrl;
    private final String queue;

    @Override
    public String toString() {
        if (queue.equals("Main")) {
            return String.format("*%s* (%s)", transportType.getName(), baseUrl);
        } else {
            return String.format("*%s* (%s) _%s_", transportType.getName(), baseUrl, queue);
        }
    }

}
