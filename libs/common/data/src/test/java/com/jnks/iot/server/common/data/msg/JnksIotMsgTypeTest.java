package com.jnks.iot.server.common.data.msg;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.ALARM;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.ALARM_DELETE;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.DEDUPLICATION_TIMEOUT_SELF_MSG;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.DELAY_TIMEOUT_SELF_MSG;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.DEVICE_PROFILE_PERIODIC_SELF_MSG;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.DEVICE_PROFILE_UPDATE_SELF_MSG;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.DEVICE_UPDATE_SELF_MSG;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.GENERATOR_NODE_SELF_MSG;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.MSG_COUNT_SELF_MSG;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.NA;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.PROVISION_FAILURE;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.PROVISION_SUCCESS;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.SEND_EMAIL;

class JnksIotMsgTypeTest {

    private static final List<JnksIotMsgType> typesWithNullRuleNodeConnection = List.of(
            ALARM,
            ALARM_DELETE,
            PROVISION_FAILURE,
            PROVISION_SUCCESS,
            SEND_EMAIL,
            GENERATOR_NODE_SELF_MSG,
            DEVICE_PROFILE_PERIODIC_SELF_MSG,
            DEVICE_PROFILE_UPDATE_SELF_MSG,
            DEVICE_UPDATE_SELF_MSG,
            DEDUPLICATION_TIMEOUT_SELF_MSG,
            DELAY_TIMEOUT_SELF_MSG,
            MSG_COUNT_SELF_MSG,
            NA
    );

    // backward-compatibility tests

    @Test
    void getRuleNodeConnectionsTest() {
        var jnksIotMsgTypes = JnksIotMsgType.values();
        for (var type : jnksIotMsgTypes) {
            if (typesWithNullRuleNodeConnection.contains(type)) {
                assertThat(type.getRuleNodeConnection()).isEqualTo(JnksIotNodeConnectionType.OTHER);
            } else {
                assertThat(type.getRuleNodeConnection()).isNotEqualTo(JnksIotNodeConnectionType.OTHER);
            }
        }
    }

    @Test
    void getRuleNodeConnectionOrElseOtherTest() {
        var jnksIotMsgTypes = JnksIotMsgType.values();
        for (var type : jnksIotMsgTypes) {
            if (typesWithNullRuleNodeConnection.contains(type)) {
                assertThat(type.getRuleNodeConnection())
                        .isEqualTo(JnksIotNodeConnectionType.OTHER);
            } else {
                assertThat(type.getRuleNodeConnection()).isNotNull()
                        .isNotEqualTo(JnksIotNodeConnectionType.OTHER);
            }
        }
    }

}
