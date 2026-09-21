package com.jnks.iot.rule.engine.math;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.Optional;

public class JnksIotMathArgumentValue {

    @Getter
    private final double value;

    private JnksIotMathArgumentValue(double value) {
        this.value = value;
    }

    public static JnksIotMathArgumentValue constant(JnksIotMathArgument arg) {
        return fromString(arg.getKey());
    }

    private static JnksIotMathArgumentValue defaultOrThrow(Double defaultValue, String error) {
        if (defaultValue != null) {
            return new JnksIotMathArgumentValue(defaultValue);
        }
        throw new RuntimeException(error);
    }

    public static JnksIotMathArgumentValue fromMessageBody(JnksIotMathArgument arg, String argKey, Optional<ObjectNode> jsonNodeOpt) {
        Double defaultValue = arg.getDefaultValue();
        if (jsonNodeOpt.isEmpty()) {
            return defaultOrThrow(defaultValue, "Message body is empty!");
        }
        var json = jsonNodeOpt.get();
        if (!json.has(argKey)) {
            return defaultOrThrow(defaultValue, "Message body has no '" + argKey + "'!");
        }
        JsonNode valueNode = json.get(argKey);
        if (valueNode.isNull()) {
            return defaultOrThrow(defaultValue, "Message body has null '" + argKey + "'!");
        }
        double value;
        if (valueNode.isNumber()) {
            value = valueNode.doubleValue();
        } else if (valueNode.isTextual()) {
            var valueNodeText = valueNode.asText();
            if (StringUtils.isNotBlank(valueNodeText)) {
                try {
                    value = Double.parseDouble(valueNode.asText());
                } catch (NumberFormatException ne) {
                    throw new RuntimeException("Can't convert value '" + valueNode.asText() + "' to double!");
                }
            } else {
                return defaultOrThrow(defaultValue, "Message value is empty for '" + argKey + "'!");
            }
        } else {
            throw new RuntimeException("Can't convert value '" + valueNode.toString() + "' to double!");
        }
        return new JnksIotMathArgumentValue(value);
    }

    public static JnksIotMathArgumentValue fromMessageMetadata(JnksIotMathArgument arg, String argKey, JnksIotMsgMetaData metaData) {
        Double defaultValue = arg.getDefaultValue();
        if (metaData == null) {
            return defaultOrThrow(defaultValue, "Message metadata is empty!");
        }
        var value = metaData.getValue(argKey);
        if (StringUtils.isEmpty(value)) {
            return defaultOrThrow(defaultValue, "Message metadata has no '" + argKey + "'!");
        }
        return fromString(value);
    }

    public static JnksIotMathArgumentValue fromLong(long value) {
        return new JnksIotMathArgumentValue(value);
    }

    public static JnksIotMathArgumentValue fromDouble(double value) {
        return new JnksIotMathArgumentValue(value);
    }

    public static JnksIotMathArgumentValue fromString(String value) {
        try {
            return new JnksIotMathArgumentValue(Double.parseDouble(value));
        } catch (NumberFormatException ne) {
            throw new RuntimeException("Can't convert value '" + value + "' to double!");
        }
    }
}
