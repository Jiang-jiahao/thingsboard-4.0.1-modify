package org.thingsboard.monitoring.util;

import lombok.SneakyThrows;
import org.thingsboard.common.util.JacksonUtil;

import java.io.InputStream;

public class ResourceUtils {

    @SneakyThrows
    public static <T> T getResource(String path, Class<T> type) {
        InputStream resource = ResourceUtils.class.getClassLoader().getResourceAsStream(path);
        if (resource == null) {
            throw new IllegalArgumentException("Resource not found for path " + path);
        }
        return JacksonUtil.OBJECT_MAPPER.readValue(resource, type);
    }

    public static InputStream getResourceAsStream(String path) {
        InputStream resource = ResourceUtils.class.getClassLoader().getResourceAsStream(path);
        if (resource == null) {
            throw new IllegalArgumentException("Resource not found for path " + path);
        }
        return resource;
    }

}
