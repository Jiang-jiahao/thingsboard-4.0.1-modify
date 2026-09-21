package com.jnks.iot.server.common.msg;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JnksIotMsgMetaDataTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final String metadataJsonStr = "{\"deviceName\":\"Test Device\",\"deviceType\":\"default\",\"ts\":\"1645112691407\"}";
    private JsonNode metadataJson;
    private Map<String, String> metadataExpected;

    @BeforeEach
    public void startInit() throws Exception {
        metadataJson = objectMapper.readValue(metadataJsonStr, JsonNode.class);
        metadataExpected = objectMapper.convertValue(metadataJson, new TypeReference<>() {
        });
    }

    @Test
    public void testScript_whenMetadataWithoutPropertiesValueNull_returnMetadataWithAllValue() {
        JnksIotMsgMetaData jnksIotMsgMetaData = new JnksIotMsgMetaData(metadataExpected);
        Map<String, String> dataActual = jnksIotMsgMetaData.values();
        assertEquals(metadataExpected.size(), dataActual.size());
    }

    @Test
    public void testScript_whenMetadataWithPropertiesValueNull_returnMetadataWithoutPropertiesValueEqualsNull() {
        metadataExpected.put("deviceName", null);
        JnksIotMsgMetaData jnksIotMsgMetaData = new JnksIotMsgMetaData(metadataExpected);
        Map<String, String> dataActual = jnksIotMsgMetaData.copy().getData();
        assertEquals(metadataExpected.size() - 1, dataActual.size());
    }
}
