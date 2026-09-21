package com.jnks.iot.server.common.msg;

import org.junit.jupiter.api.Test;
import com.jnks.iot.server.common.data.JavaSerDesUtil;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.RuleNodeId;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class JnksIotMsgProcessingStackItemTest {

    @Test
    void testSerialization() {
        JnksIotMsgProcessingStackItem item = new JnksIotMsgProcessingStackItem(new RuleChainId(UUID.randomUUID()), new RuleNodeId(UUID.randomUUID()));
        byte[] bytes = JavaSerDesUtil.encode(item);
        JnksIotMsgProcessingStackItem itemDecoded = JavaSerDesUtil.decode(bytes);
        assertThat(item).isEqualTo(itemDecoded);
    }

}
