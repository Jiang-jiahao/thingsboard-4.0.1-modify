package org.thingsboard.server.actors;

import lombok.Getter;
import lombok.ToString;

/**
 * 执行消息处理错误之后的策略
 * 停止或直接继续执行后续消息处理
 */
@ToString
public class ProcessFailureStrategy {

    @Getter
    private boolean stop;

    private ProcessFailureStrategy(boolean stop) {
        this.stop = stop;
    }

    public static ProcessFailureStrategy stop() {
        return new ProcessFailureStrategy(true);
    }

    public static ProcessFailureStrategy resume() {
        return new ProcessFailureStrategy(false);
    }
}
