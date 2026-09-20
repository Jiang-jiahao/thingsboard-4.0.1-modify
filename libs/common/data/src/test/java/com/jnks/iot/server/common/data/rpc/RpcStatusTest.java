package com.jnks.iot.server.common.data.rpc;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static com.jnks.iot.server.common.data.rpc.RpcStatus.DELIVERED;
import static com.jnks.iot.server.common.data.rpc.RpcStatus.QUEUED;
import static com.jnks.iot.server.common.data.rpc.RpcStatus.SENT;

class RpcStatusTest {

    private static final List<RpcStatus> pushDeleteNotificationToCoreStatuses = List.of(
            QUEUED,
            SENT,
            DELIVERED
    );

    @Test
    void isPushDeleteNotificationToCoreStatusTest() {
        var rpcStatuses = RpcStatus.values();
        for (var status : rpcStatuses) {
            if (pushDeleteNotificationToCoreStatuses.contains(status)) {
                assertThat(status.isPushDeleteNotificationToCore()).isTrue();
            } else {
                assertThat(status.isPushDeleteNotificationToCore()).isFalse();
            }
        }
    }

}
