package org.thingsboard.server.cache;

import lombok.Data;

@Data
public class CacheSpecs {
    /**
     * 缓存条目存活时间（分钟）
     */
    private Integer timeToLiveInMinutes;

    /**
     *  缓存最大容量
     */
    private Integer maxSize;
}
