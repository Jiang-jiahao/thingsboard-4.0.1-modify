package org.thingsboard.server.common.msg.gateway.metrics;

public record GatewayMetadata(String connector, long receivedTs, long publishedTs) {
}
