package org.thingsboard.server.actors;

import lombok.Data;

/**
 * actor系统配置
 */
@Data
public class TbActorSystemSettings {

    /**
     * actor处理的吞吐量
     */
    private final int actorThroughput;

    /**
     * 调度器线程池大小
     */
    private final int schedulerPoolSize;

    /**
     * actor初始化失败最大尝试次数
     */
    private final int maxActorInitAttempts;

}
