package com.jnks.iot.server.common.transport.activity;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import com.jnks.iot.server.common.transport.activity.strategy.ActivityStrategy;
import com.jnks.iot.server.common.scheduler.SchedulerComponent;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public abstract class AbstractActivityManager<Key, Metadata> implements ActivityManager<Key, Metadata> {

    private final ConcurrentMap<Key, ActivityStateWrapper> states = new ConcurrentHashMap<>();

    @Autowired
    protected SchedulerComponent scheduler;

    @Data
    private class ActivityStateWrapper {

        private volatile ActivityState<Metadata> state;
        private volatile long lastReportedTime;
        private volatile ActivityStrategy strategy;

    }

    protected void init() {
        var reportingPeriodMillis = getReportingPeriodMillis();
        scheduler.scheduleAtFixedRate(this::onReportingPeriodEnd, new Random().nextInt((int) reportingPeriodMillis), reportingPeriodMillis, TimeUnit.MILLISECONDS);
    }

    protected abstract long getReportingPeriodMillis();

    protected abstract ActivityStrategy getStrategy();

    protected abstract ActivityState<Metadata> updateState(Key key, ActivityState<Metadata> state);

    protected abstract boolean hasExpired(long lastRecordedTime);

    protected abstract void onStateExpiry(Key key, Metadata metadata);

    protected abstract void reportActivity(Key key, Metadata metadata, long timeToReport, ActivityReportCallback<Key> callback);

    /**
     * 该 key 对应的会话是否豁免"不活动过期"。
     * <p>
     * 豁免会话（UDP/HTTP 给从不开口的设备建的**虚拟**下发/采集会话）同时**不产生活动上报** ——
     * 它本来就不是设备连接，Core 侧也没有它的会话，上报过去只会被丢弃。
     */
    protected boolean isExpiryExempt(Key key) {
        return false;
    }

    @Override
    public void onActivity(Key key, Metadata metadata, long newLastRecordedTime) {
        if (key == null) {
            log.error("Failed to process activity event: provided activity key is null.");
            return;
        }
        log.debug("Received activity event for key: [{}]. Event time: [{}].", key, newLastRecordedTime);

        var shouldReport = new AtomicBoolean(false);
        var lastRecordedTime = new AtomicLong();
        var lastReportedTime = new AtomicLong();

        states.compute(key, (__, stateWrapper) -> {
            if (stateWrapper == null) {
                ActivityState<Metadata> newState = new ActivityState<>();
                stateWrapper = new ActivityStateWrapper();
                stateWrapper.setState(newState);
                stateWrapper.setStrategy(getStrategy());
            }
            var state = stateWrapper.getState();
            state.setMetadata(metadata);
            if (state.getLastRecordedTime() < newLastRecordedTime) {
                state.setLastRecordedTime(newLastRecordedTime);
            }
            shouldReport.set(stateWrapper.getStrategy().onActivity());
            lastRecordedTime.set(state.getLastRecordedTime());
            lastReportedTime.set(stateWrapper.getLastReportedTime());
            return stateWrapper;
        });

        // shouldReport 放前面短路：默认策略（LAST）下它是 false，这样每个报文都不用去查会话表。
        if (shouldReport.get() && !isExpiryExempt(key) && lastReportedTime.get() < lastRecordedTime.get()) {
            log.debug("Going to report first activity event for key: [{}]. Event time: [{}].", key, lastRecordedTime.get());
            reportActivity(key, metadata, lastRecordedTime.get(), new ActivityReportCallback<>() {
                @Override
                public void onSuccess(Key key, long reportedTime) {
                    updateLastReportedTime(key, reportedTime);
                }

                @Override
                public void onFailure(Key key, Throwable t) {
                    log.debug("Failed to report first activity event for key: [{}]. Event time: [{}].", key, lastRecordedTime.get(), t);
                }
            });
        }
    }

    @Override
    public void onReportingPeriodEnd() {
        log.debug("Going to end reporting period.");
        for (Map.Entry<Key, ActivityStateWrapper> entry : states.entrySet()) {
            Key key = entry.getKey();
            ActivityStateWrapper stateWrapper = entry.getValue();
            try {
                reportLastEvent(key, stateWrapper);
            } catch (Exception e) {
                log.error("Failed to report last activity event on reporting period end for key: [{}]. State: [{}].", key, stateWrapper, e);
            }
        }
    }

    private void reportLastEvent(Key key, ActivityStateWrapper stateWrapper) {
        var currentState = stateWrapper.getState();

        long lastRecordedTime = currentState.getLastRecordedTime();
        long lastReportedTime = stateWrapper.getLastReportedTime();
        var metadata = currentState.getMetadata();

        boolean hasExpired;
        boolean shouldReport;

        var updatedState = updateState(key, currentState);
        boolean expiryExempt = isExpiryExempt(key);
        if (updatedState != null) {
            stateWrapper.setState(updatedState);
            lastRecordedTime = updatedState.getLastRecordedTime();
            metadata = updatedState.getMetadata();
            hasExpired = !expiryExempt && hasExpired(lastRecordedTime);
            shouldReport = stateWrapper.getStrategy().onReportingPeriodEnd();
        } else {
            states.remove(key);
            hasExpired = false;
            shouldReport = false;
        }

        if (hasExpired) {
            states.remove(key);
            onStateExpiry(key, metadata);
            shouldReport = true;
        }

        if (!expiryExempt && shouldReport && lastReportedTime < lastRecordedTime) {
            long timeToReport = lastRecordedTime;
            log.debug("Going to report last activity event for key: [{}]. Event time: [{}].", key, timeToReport);
            reportActivity(key, metadata, timeToReport, new ActivityReportCallback<>() {
                @Override
                public void onSuccess(Key key, long reportedTime) {
                    updateLastReportedTime(key, reportedTime);
                }

                @Override
                public void onFailure(Key key, Throwable t) {
                    log.debug("Failed to report last activity event for key: [{}]. Event time: [{}].", key, timeToReport, t);
                }
            });
        }
    }

    @Override
    public long getLastRecordedTime(Key key) {
        ActivityStateWrapper stateWrapper = states.get(key);
        return stateWrapper == null ? 0L : stateWrapper.getState().getLastRecordedTime();
    }

    private void updateLastReportedTime(Key key, long newLastReportedTime) {
        states.computeIfPresent(key, (__, stateWrapper) -> {
            stateWrapper.setLastReportedTime(Math.max(stateWrapper.getLastReportedTime(), newLastReportedTime));
            return stateWrapper;
        });
    }

}
