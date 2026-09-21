package com.jnks.iot.rule.engine.api.util;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
public class JnksIotNodeUtilsTest {

    private static final String DATA_VARIABLE_TEMPLATE = "$[%s]";
    private static final String METADATA_VARIABLE_TEMPLATE = "${%s}";

    @Test
    public void testSimpleReplacement() {
        String pattern = "ABC ${metadata_key} $[data_key]";
        JnksIotMsgMetaData md = new JnksIotMsgMetaData();
        md.putValue("metadata_key", "metadata_value");

        ObjectNode node = JacksonUtil.newObjectNode();
        node.put("data_key", "data_value");

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(TenantId.SYS_TENANT_ID)
                .copyMetaData(md)
                .data(JacksonUtil.toString(node))
                .build();
        String result = JnksIotNodeUtils.processPattern(pattern, msg);
        Assertions.assertEquals("ABC metadata_value data_value", result);
    }

    @Test
    public void testNoReplacement() {
        String pattern = "ABC ${metadata_key} $[data_key]";
        JnksIotMsgMetaData md = new JnksIotMsgMetaData();
        md.putValue("key", "metadata_value");

        ObjectNode node = JacksonUtil.newObjectNode();
        node.put("key", "data_value");

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(TenantId.SYS_TENANT_ID)
                .copyMetaData(md)
                .data(JacksonUtil.toString(node))
                .build();
        String result = JnksIotNodeUtils.processPattern(pattern, msg);
        Assertions.assertEquals(pattern, result);
    }

    @Test
    public void testSameKeysReplacement() {
        String pattern = "ABC ${key} $[key]";
        JnksIotMsgMetaData md = new JnksIotMsgMetaData();
        md.putValue("key", "metadata_value");

        ObjectNode node = JacksonUtil.newObjectNode();
        node.put("key", "data_value");

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(TenantId.SYS_TENANT_ID)
                .copyMetaData(md)
                .data(JacksonUtil.toString(node))
                .build();
        String result = JnksIotNodeUtils.processPattern(pattern, msg);
        Assertions.assertEquals("ABC metadata_value data_value", result);
    }

    @Test
    public void testComplexObjectReplacement() {
        String pattern = "ABC ${key} $[key1.key2.key3]";
        JnksIotMsgMetaData md = new JnksIotMsgMetaData();
        md.putValue("key", "metadata_value");

        ObjectNode key2Node = JacksonUtil.newObjectNode();
        key2Node.put("key3", "value3");

        ObjectNode key1Node = JacksonUtil.newObjectNode();
        key1Node.set("key2", key2Node);


        ObjectNode node = JacksonUtil.newObjectNode();
        node.set("key1", key1Node);

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(TenantId.SYS_TENANT_ID)
                .copyMetaData(md)
                .data(JacksonUtil.toString(node))
                .build();
        String result = JnksIotNodeUtils.processPattern(pattern, msg);
        Assertions.assertEquals("ABC metadata_value value3", result);
    }

    @Test
    public void testArrayReplacementDoesNotWork() {
        String pattern = "ABC ${key} $[key1.key2[0].key3]";
        JnksIotMsgMetaData md = new JnksIotMsgMetaData();
        md.putValue("key", "metadata_value");

        ObjectNode key2Node = JacksonUtil.newObjectNode();
        key2Node.put("key3", "value3");

        ObjectNode key1Node = JacksonUtil.newObjectNode();
        key1Node.set("key2", key2Node);


        ObjectNode node = JacksonUtil.newObjectNode();
        node.set("key1", key1Node);

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(TenantId.SYS_TENANT_ID)
                .copyMetaData(md)
                .data(JacksonUtil.toString(node))
                .build();
        String result = JnksIotNodeUtils.processPattern(pattern, msg);
        Assertions.assertEquals("ABC metadata_value $[key1.key2[0].key3]", result);
    }

    @Test
    public void givenKey_whenFormatDataVarTemplate_thenReturnTheSameStringAsFormat() {
        assertThat(JnksIotNodeUtils.formatDataVarTemplate("key"), is("$[key]"));
        assertThat(JnksIotNodeUtils.formatDataVarTemplate("key"), is(String.format(DATA_VARIABLE_TEMPLATE, "key")));

        assertThat(JnksIotNodeUtils.formatDataVarTemplate(""), is("$[]"));
        assertThat(JnksIotNodeUtils.formatDataVarTemplate(""), is(String.format(DATA_VARIABLE_TEMPLATE, "")));

        assertThat(JnksIotNodeUtils.formatDataVarTemplate(null), is("$[null]"));
        assertThat(JnksIotNodeUtils.formatDataVarTemplate(null), is(String.format(DATA_VARIABLE_TEMPLATE, (String) null)));
    }

    @Test
    public void givenKey_whenFormatMetadataVarTemplate_thenReturnTheSameStringAsFormat() {
        assertThat(JnksIotNodeUtils.formatMetadataVarTemplate("key"), is("${key}"));
        assertThat(JnksIotNodeUtils.formatMetadataVarTemplate("key"), is(String.format(METADATA_VARIABLE_TEMPLATE, "key")));

        assertThat(JnksIotNodeUtils.formatMetadataVarTemplate(""), is("${}"));
        assertThat(JnksIotNodeUtils.formatMetadataVarTemplate(""), is(String.format(METADATA_VARIABLE_TEMPLATE, "")));

        assertThat(JnksIotNodeUtils.formatMetadataVarTemplate(null), is("${null}"));
        assertThat(JnksIotNodeUtils.formatMetadataVarTemplate(null), is(String.format(METADATA_VARIABLE_TEMPLATE, (String) null)));
    }
}
