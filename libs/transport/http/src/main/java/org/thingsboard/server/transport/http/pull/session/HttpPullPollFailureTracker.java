package org.thingsboard.server.transport.http.pull.session;

import java.util.concurrent.TimeUnit;

/**
 * 单个 poll 请求的失败状态：判定这次失败值不值得输出。
 * <p>
 * 厂家不可达、鉴权失效、路径配错这类问题会按轮询间隔持续失败，若每次都打 WARN + 堆栈，
 * 几分钟就能把日志刷满。策略：首次失败与「错误内容变化」时完整输出，同一种错误持续期间
 * 只在 {@link #REPORT_INTERVAL_MS} 窗口内打一条摘要，其余降到 DEBUG。
 */
public class HttpPullPollFailureTracker {

    private static final long REPORT_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);

    private String signature;
    private int consecutive;
    private long lastReportedAt;

    public synchronized Report onFailure(String detail, long now) {
        boolean firstOrChanged = signature == null || !signature.equals(detail);
        if (firstOrChanged) {
            signature = detail;
        }
        consecutive++;
        boolean reported = firstOrChanged || now - lastReportedAt >= REPORT_INTERVAL_MS;
        if (reported) {
            lastReportedAt = now;
        }
        return new Report(reported, firstOrChanged, consecutive);
    }

    /** @return 恢复前的连续失败次数；0 表示此前没有失败。 */
    public synchronized int reset() {
        int previous = consecutive;
        signature = null;
        consecutive = 0;
        lastReportedAt = 0;
        return previous;
    }

    /**
     * @param reported      本次是否应该输出（含 errorEvent）
     * @param firstOrChanged 是否是该错误的第一次（此时输出完整信息，含堆栈）
     * @param consecutive   含本次在内的连续失败次数
     */
    public record Report(boolean reported, boolean firstOrChanged, int consecutive) {
    }
}
