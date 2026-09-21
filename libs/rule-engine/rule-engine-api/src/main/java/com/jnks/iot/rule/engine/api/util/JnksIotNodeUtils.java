package com.jnks.iot.rule.engine.api.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.util.CollectionUtils;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by ashvayka on 19.01.18.
 */
public class JnksIotNodeUtils {

    private static final Pattern DATA_PATTERN = Pattern.compile("(\\$\\[)(.*?)(])");

    public static <T> T convert(JnksIotNodeConfiguration configuration, Class<T> clazz) throws JnksIotNodeException {
        try {
            return JacksonUtil.treeToValue(configuration.getData(), clazz);
        } catch (IllegalArgumentException e) {
            throw new JnksIotNodeException(e, true);
        }
    }

    public static List<String> processPatterns(List<String> patterns, JnksIotMsg jnksIotMsg) {
        if (!CollectionUtils.isEmpty(patterns)) {
            return patterns.stream().map(p -> processPattern(p, jnksIotMsg)).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    public static String processPattern(String pattern, JnksIotMsg jnksIotMsg) {
        try {
            String result = processPattern(pattern, jnksIotMsg.getMetaData());
            JsonNode json = JacksonUtil.toJsonNode(jnksIotMsg.getData());
            if (json.isObject()) {
                Matcher matcher = DATA_PATTERN.matcher(result);
                while (matcher.find()) {
                    String group = matcher.group(2);
                    String[] keys = group.split("\\.");
                    JsonNode jsonNode = json;
                    for (String key : keys) {
                        if (!StringUtils.isEmpty(key) && jsonNode != null) {
                            jsonNode = jsonNode.get(key);
                        } else {
                            jsonNode = null;
                            break;
                        }
                    }

                    if (jsonNode != null && jsonNode.isValueNode()) {
                        result = result.replace(formatDataVarTemplate(group), jsonNode.asText());
                    }
                }
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to process pattern!", e);
        }
    }

    @Deprecated(since = "3.6.1", forRemoval = true)
    public static List<String> processPatterns(List<String> patterns, JnksIotMsgMetaData metaData) {
        if (!CollectionUtils.isEmpty(patterns)) {
            return patterns.stream().map(p -> processPattern(p, metaData)).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    public static String processPattern(String pattern, JnksIotMsgMetaData metaData) {
        return processTemplate(pattern, metaData.values());
    }

    public static String processTemplate(String template, Map<String, String> data) {
        String result = template;
        for (Map.Entry<String, String> kv : data.entrySet()) {
            result = processVar(result, kv.getKey(), kv.getValue());
        }
        return result;
    }

    private static String processVar(String pattern, String key, String val) {
        return pattern.replace(formatMetadataVarTemplate(key), val);
    }

    static String formatDataVarTemplate(String key) {
        return "$[" + key + ']';
    }

    static String formatMetadataVarTemplate(String key) {
        return "${" + key + '}';
    }
}
