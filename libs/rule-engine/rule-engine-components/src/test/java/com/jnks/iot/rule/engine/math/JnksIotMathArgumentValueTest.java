package com.jnks.iot.rule.engine.math;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class JnksIotMathArgumentValueTest {

    @Test
    public void test_fromMessageBody_then_defaultValue() {
        JnksIotMathArgument jnksIotMathArgument = new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "TestKey");
        jnksIotMathArgument.setDefaultValue(5.0);
        JnksIotMathArgumentValue result = JnksIotMathArgumentValue.fromMessageBody(jnksIotMathArgument, jnksIotMathArgument.getKey(), Optional.ofNullable(JacksonUtil.newObjectNode()));
        Assertions.assertEquals(5.0, result.getValue(), 0d);
    }

    @Test
    public void test_fromMessageBody_then_emptyBody() {
        JnksIotMathArgument jnksIotMathArgument = new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "TestKey");
        Throwable thrown = assertThrows(RuntimeException.class, () -> {
            JnksIotMathArgumentValue result = JnksIotMathArgumentValue.fromMessageBody(jnksIotMathArgument, jnksIotMathArgument.getKey(), Optional.empty());
        });
        Assertions.assertNotNull(thrown.getMessage());
    }

    @Test
    public void test_fromMessageBody_then_noKey() {
        JnksIotMathArgument jnksIotMathArgument = new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "TestKey");
        Throwable thrown = assertThrows(RuntimeException.class, () -> JnksIotMathArgumentValue.fromMessageBody(jnksIotMathArgument, jnksIotMathArgument.getKey(), Optional.ofNullable(JacksonUtil.newObjectNode())));
        Assertions.assertNotNull(thrown.getMessage());
    }

    @Test
    public void test_fromMessageBody_then_valueEmpty() {
        JnksIotMathArgument jnksIotMathArgument = new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "TestKey");
        ObjectNode msgData = JacksonUtil.newObjectNode();
        msgData.putNull("TestKey");

        //null value
        Throwable thrown = assertThrows(RuntimeException.class, () -> JnksIotMathArgumentValue.fromMessageBody(jnksIotMathArgument, jnksIotMathArgument.getKey(), Optional.of(msgData)));
        Assertions.assertNotNull(thrown.getMessage());

        //empty value
        msgData.put("TestKey", "");
        thrown = assertThrows(RuntimeException.class, () -> JnksIotMathArgumentValue.fromMessageBody(jnksIotMathArgument, jnksIotMathArgument.getKey(), Optional.of(msgData)));
        Assertions.assertNotNull(thrown.getMessage());
    }

    @Test
    public void test_fromMessageBody_then_valueCantConvert_to_double() {
        JnksIotMathArgument jnksIotMathArgument = new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "TestKey");
        ObjectNode msgData = JacksonUtil.newObjectNode();
        msgData.put("TestKey", "Test");

        //string value
        Throwable thrown = assertThrows(RuntimeException.class, () -> JnksIotMathArgumentValue.fromMessageBody(jnksIotMathArgument, jnksIotMathArgument.getKey(), Optional.of(msgData)));
        Assertions.assertNotNull(thrown.getMessage());

        //object value
        msgData.set("TestKey", JacksonUtil.newObjectNode());
        thrown = assertThrows(RuntimeException.class, () -> JnksIotMathArgumentValue.fromMessageBody(jnksIotMathArgument, jnksIotMathArgument.getKey(), Optional.of(msgData)));
        Assertions.assertNotNull(thrown.getMessage());
    }

    @Test
    public void test_fromMessageMetadata_then_noKey() {
        JnksIotMathArgument jnksIotMathArgument = new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "TestKey");
        Throwable thrown = assertThrows(RuntimeException.class, () -> JnksIotMathArgumentValue.fromMessageMetadata(jnksIotMathArgument, jnksIotMathArgument.getKey(), new JnksIotMsgMetaData()));
        Assertions.assertNotNull(thrown.getMessage());
    }

    @Test
    public void test_fromMessageMetadata_then_valueEmpty() {
        JnksIotMathArgument jnksIotMathArgument = new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "TestKey");
        Throwable thrown = assertThrows(RuntimeException.class, () -> JnksIotMathArgumentValue.fromMessageMetadata(jnksIotMathArgument, jnksIotMathArgument.getKey(), null));
        Assertions.assertNotNull(thrown.getMessage());
    }

    @Test
    public void test_fromString_thenOK() {
        var value = "5.0";
        JnksIotMathArgumentValue result = JnksIotMathArgumentValue.fromString(value);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5.0, result.getValue(), 0d);
    }

    @Test
    public void test_fromString_then_failure() {
        var value = "Test";
        Throwable thrown = assertThrows(RuntimeException.class, () -> JnksIotMathArgumentValue.fromString(value));
        Assertions.assertNotNull(thrown.getMessage());
    }
}
